<?php

declare(strict_types=1);

namespace Kode\Queue\Driver\Redis;

/**
 * Redis 客户端最小契约。
 *
 * v1 直接把 `Predis\Client` 写死在驱动里，装了 `ext-redis` 的用户被迫再装一遍 Predis。
 * v2 把「用什么客户端」收敛到这层适配：驱动只依赖本接口，
 * 具体是 Predis 还是 phpredis 由 {@see RedisClientFactory} 在运行时决定。
 *
 * 接口刻意只保留 6 个方法——因为绝大多数队列操作都通过 Lua 脚本一次性完成，
 * 不需要把 Redis 上百条命令全部代理一遍。
 */
interface RedisClientInterface
{
    /**
     * 执行 Lua 脚本（内部自动做 EVALSHA 缓存，NOSCRIPT 时回退 EVAL）。
     *
     * @param list<string>       $keys
     * @param list<string|int|float> $argv
     */
    public function evaluate(string $script, array $keys = [], array $argv = []): mixed;

    /**
     * 有序集合基数。
     */
    public function zcard(string $key): int;

    /**
     * 取有序集合分值最小的成员（peek 用）。
     */
    public function firstMember(string $key): ?string;

    /**
     * 列表长度（死信队列统计用）。
     */
    public function llen(string $key): int;

    public function ping(): bool;

    public function disconnect(): void;

    /**
     * 底层客户端实例（需要执行本接口未覆盖的命令时使用）。
     */
    public function raw(): object;
}
