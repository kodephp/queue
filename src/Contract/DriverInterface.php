<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

use Closure;
use Kode\Queue\Enum\Capability;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;

/**
 * 队列驱动契约。
 *
 * 相比 v1，本接口有三处根本性变化：
 *
 * 1. **对象化**：收发的是 {@see Job} / {@see ReservedJob} 值对象，而非裸 JSON 字符串，
 *    序列化职责下沉到 {@see SerializerInterface}，驱动只关心「怎么存怎么取」。
 * 2. **可确认**：新增 `ack()` / `fail()`，配合 `ReservedJob` 的回执句柄实现
 *    「至少一次投递」语义；v1 的 `delete()` 在多数驱动里只是 `return true` 的空壳。
 * 3. **能力可探测**：`supports()` 让上层在调用前就知道该后端能不能延迟、能不能优先级，
 *    从而选择降级方案而不是等着抛异常。
 */
interface DriverInterface
{
    /**
     * 驱动类型。
     */
    public function type(): DriverType;

    /**
     * 是否支持某项能力。
     */
    public function supports(Capability $capability): bool;

    // ------------------------------------------------------------------
    // 生产
    // ------------------------------------------------------------------

    /**
     * 投递单个任务（含延迟：由 `$job->availableAt` 决定）。
     *
     * @return string 任务 ID
     */
    public function push(Job $job): string;

    /**
     * 批量投递。驱动应尽量使用后端的原生批量能力（pipeline / 多值 INSERT）。
     *
     * @param  iterable<Job> $jobs
     * @return list<string>  任务 ID 列表，顺序与入参一致
     */
    public function pushBatch(iterable $jobs): array;

    // ------------------------------------------------------------------
    // 消费
    // ------------------------------------------------------------------

    /**
     * 拉取并预留一个任务。
     *
     * @param string $queue   队列名
     * @param float  $timeout 阻塞等待秒数；0 表示立即返回（非阻塞）
     */
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob;

    /**
     * 窥视队首任务但不消费（不支持时返回 null）。
     */
    public function peek(string $queue): ?Job;

    /**
     * 确认任务已完成，从后端彻底移除。
     */
    public function ack(ReservedJob $job): bool;

    /**
     * 将任务放回队列等待重试。
     *
     * @param int $delay 延迟秒数
     */
    public function release(ReservedJob $job, int $delay = 0): bool;

    /**
     * 标记任务失败（支持死信的后端会 bury / 投递到 DLQ，否则等价于 ack）。
     */
    public function fail(ReservedJob $job, \Throwable $reason): bool;

    // ------------------------------------------------------------------
    // 管理
    // ------------------------------------------------------------------

    /**
     * 可消费任务数量。
     */
    public function size(string $queue): int;

    /**
     * 清空队列，返回被清除的任务数（不支持时返回 0）。
     */
    public function clear(string $queue): int;

    /**
     * 统计快照。
     */
    public function stats(string $queue): QueueStats;

    /**
     * 把超时未确认的预留任务重新放回队列，返回恢复条数。
     *
     * 这是 v2 新增的「可见性超时」机制：Worker 崩溃时任务不会永久丢失。
     */
    public function reclaimExpired(string $queue): int;

    // ------------------------------------------------------------------
    // 事务与连接
    // ------------------------------------------------------------------

    /**
     * 在事务中执行回调；异常自动回滚，正常提交并返回回调结果。
     *
     * 相比 v1 手工 begin/commit/rollback，闭包式事务不会漏掉回滚分支。
     *
     * @template T
     * @param  Closure(static): T $callback
     * @return T
     */
    public function transaction(Closure $callback): mixed;

    /**
     * 连接是否可用（惰性连接的驱动可用于健康检查）。
     */
    public function isConnected(): bool;

    /**
     * 重建连接（长驻进程中断线重连用）。
     */
    public function reconnect(): void;

    /**
     * 关闭连接释放资源。
     */
    public function close(): void;
}
