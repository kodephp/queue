<?php

declare(strict_types=1);

namespace Kode\Queue\Driver\Redis;

/**
 * Redis 队列的原子脚本集合。
 *
 * 为什么全部走 Lua：队列的每个动作都是「读—判断—写」的复合操作，
 * 用多条命令拼出来，在多 Worker 并发下必然出现同一个任务被两个进程同时取走。
 * v1 用 `LPOP` + PHP 侧判断，正是这个问题的典型现场。
 * Lua 脚本在 Redis 单线程内一次性执行，天然互斥，且只有一次 RTT。
 *
 * 约定的键布局（`{prefix}{queue}` 为基名）：
 *
 * | 键                  | 类型 | 分值含义                       |
 * |---------------------|------|--------------------------------|
 * | `{base}`            | ZSET | 就绪队列，分值 = 到达时间 - 优先级偏移 |
 * | `{base}:delayed`    | ZSET | 延迟队列，分值 = 可执行时间戳   |
 * | `{base}:reserved`   | ZSET | 预留队列，分值 = 可见性超时截止 |
 * | `{base}:failed`     | LIST | 可选死信队列                   |
 */
final class LuaScripts
{
    /** 单次搬运的最大条数，避免长脚本阻塞 Redis */
    public const int MIGRATE_LIMIT = 128;

    /**
     * 公共函数片段：按负载中的 priority 计算就绪分值。
     *
     * 使用 `pcall` 包裹 `cjson.decode`，非 JSON 序列化器（如 igbinary）
     * 会安全退化为纯 FIFO，而不是让整个脚本报错。
     */
    private const string HELPERS = <<<'LUA'
        local function score_of(payload, base)
            local ok, job = pcall(cjson.decode, payload)
            if ok and type(job) == 'table' and job.priority ~= nil then
                local p = tonumber(job.priority)
                if p ~= nil then
                    return base - p * 10
                end
            end
            return base
        end

        local function migrate(src, dst, now, limit)
            local items = redis.call('ZRANGEBYSCORE', src, '-inf', now, 'LIMIT', 0, limit)
            local n = #items
            if n == 0 then
                return 0
            end
            for i = 1, n do
                redis.call('ZADD', dst, score_of(items[i], now), items[i])
            end
            redis.call('ZREM', src, unpack(items))
            return n
        end
        LUA;

    /**
     * 批量投递：ARGV 以 (payload, score, delayedFlag) 三元组重复。
     *
     * KEYS[1]=ready KEYS[2]=delayed
     */
    public const string PUSH = <<<'LUA'
        local pushed = 0
        for i = 1, #ARGV, 3 do
            local target = KEYS[1]
            if ARGV[i + 2] == '1' then
                target = KEYS[2]
            end
            redis.call('ZADD', target, tonumber(ARGV[i + 1]), ARGV[i])
            pushed = pushed + 1
        end
        return pushed
        LUA;

    /**
     * 取出并预留一个任务。
     *
     * 一次脚本内完成四件事：搬运到期延迟任务 → 回收超时预留任务 →
     * 弹出分值最小者 → 写入预留队列并把 attempts 自增。
     *
     * KEYS[1]=ready KEYS[2]=delayed KEYS[3]=reserved
     * ARGV[1]=now ARGV[2]=可见性超时截止 ARGV[3]=搬运上限
     *
     * @return array{0: string, 1: int}|false [预留后的负载, attempts 是否已由脚本自增]
     */
    public static function reserve(): string
    {
        return self::HELPERS . <<<'LUA'

            local now = tonumber(ARGV[1])
            local limit = tonumber(ARGV[3])

            migrate(KEYS[2], KEYS[1], now, limit)
            migrate(KEYS[3], KEYS[1], now, limit)

            local popped = redis.call('ZRANGE', KEYS[1], 0, 0)
            if #popped == 0 then
                return false
            end

            local payload = popped[1]
            redis.call('ZREM', KEYS[1], payload)

            local rewritten = 0
            local ok, job = pcall(cjson.decode, payload)
            if ok and type(job) == 'table' then
                job.attempts = (tonumber(job.attempts) or 0) + 1
                job.status = 'reserved'
                local encoded_ok, encoded = pcall(cjson.encode, job)
                if encoded_ok then
                    payload = encoded
                    rewritten = 1
                end
            end

            redis.call('ZADD', KEYS[3], tonumber(ARGV[2]), payload)
            return { payload, rewritten }
            LUA;
    }

    /**
     * 回收超时未确认的预留任务。
     *
     * KEYS[1]=ready KEYS[2]=reserved
     * ARGV[1]=now ARGV[2]=搬运上限
     */
    public static function reclaim(): string
    {
        return self::HELPERS . <<<'LUA'

            return migrate(KEYS[2], KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]))
            LUA;
    }

    /**
     * 确认完成：从预留队列移除。
     *
     * KEYS[1]=reserved ARGV[1]=原始负载
     */
    public const string ACK = <<<'LUA'
        return redis.call('ZREM', KEYS[1], ARGV[1])
        LUA;

    /**
     * 放回队列重试：移除旧预留记录并写入新负载，原子完成避免任务凭空消失。
     *
     * KEYS[1]=ready KEYS[2]=delayed KEYS[3]=reserved
     * ARGV[1]=旧负载 ARGV[2]=新负载 ARGV[3]=分值 ARGV[4]=是否延迟
     */
    public const string RELEASE = <<<'LUA'
        local removed = redis.call('ZREM', KEYS[3], ARGV[1])
        local target = KEYS[1]
        if ARGV[4] == '1' then
            target = KEYS[2]
        end
        redis.call('ZADD', target, tonumber(ARGV[3]), ARGV[2])
        return removed
        LUA;

    /**
     * 标记失败：移出预留队列，可选写入死信列表并按上限裁剪。
     *
     * KEYS[1]=reserved KEYS[2]=failed
     * ARGV[1]=原始负载 ARGV[2]=死信负载（空串表示不留档）ARGV[3]=死信保留条数
     */
    public const string FAIL = <<<'LUA'
        local removed = redis.call('ZREM', KEYS[1], ARGV[1])
        if ARGV[2] ~= '' then
            redis.call('LPUSH', KEYS[2], ARGV[2])
            local keep = tonumber(ARGV[3])
            if keep and keep > 0 then
                redis.call('LTRIM', KEYS[2], 0, keep - 1)
            end
        end
        return removed
        LUA;

    /**
     * 清空队列并返回被清除的任务总数。
     *
     * KEYS[1]=ready KEYS[2]=delayed KEYS[3]=reserved
     */
    public const string CLEAR = <<<'LUA'
        local total = 0
        for i = 1, 3 do
            total = total + redis.call('ZCARD', KEYS[i])
        end
        redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
        return total
        LUA;

    /**
     * 统计快照，一次 RTT 拿齐四个维度。
     *
     * KEYS[1]=ready KEYS[2]=delayed KEYS[3]=reserved KEYS[4]=failed
     * ARGV[1]=now（用于区分 ready 中尚未到期的条目，恒为 0 表示不区分）
     */
    public const string STATS = <<<'LUA'
        local failed = -1
        if redis.call('EXISTS', KEYS[4]) == 1 then
            failed = redis.call('LLEN', KEYS[4])
        end
        return {
            redis.call('ZCARD', KEYS[1]),
            redis.call('ZCARD', KEYS[2]),
            redis.call('ZCARD', KEYS[3]),
            failed
        }
        LUA;

    private function __construct()
    {
    }
}
