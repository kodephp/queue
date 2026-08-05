<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

use Closure;
use DateInterval;
use DateTimeInterface;
use Generator;
use Kode\Queue\Enum\Capability;
use Kode\Queue\Enum\Priority;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\PendingDispatch;
use Kode\Queue\Support\QueueStats;

/**
 * 面向业务的队列门面契约。
 *
 * 设计取向：**默认极简，进阶可控**。
 * 90% 的场景一行 `push()` 即可；需要精细控制时用 `job()` 拿到流式构建器。
 */
interface QueueInterface
{
    /**
     * 投递任务。
     *
     * @param string|object        $job   任务名 / 任务对象（带 #[AsJob] 属性的类实例）
     * @param array<string, mixed> $data  业务数据
     * @param string|null          $queue 队列名，null 表示默认队列
     *
     * @return string 任务 ID
     */
    public function push(string|object $job, array $data = [], ?string $queue = null): string;

    /**
     * 投递一个已构建好的 {@see Job}（`PendingDispatch::dispatch()` 的落点）。
     *
     * @return string 任务 ID
     */
    public function pushJob(Job $job): string;

    /**
     * 投递到指定队列（参数顺序更自然的语法糖）。
     *
     * @param array<string, mixed> $data
     */
    public function pushOn(string $queue, string|object $job, array $data = []): string;

    /**
     * 延迟投递。
     *
     * @param int|DateInterval|DateTimeInterface $delay 秒数 / 时间间隔 / 绝对时间
     * @param array<string, mixed>               $data
     */
    public function later(
        int|DateInterval|DateTimeInterface $delay,
        string|object $job,
        array $data = [],
        ?string $queue = null,
    ): string;

    /**
     * 批量投递。
     *
     * 每个条目可以是三种形态之一：
     *
     * - `string`：任务名或任务类名，负载取公共 `$data`
     * - `Job`：现成的任务对象
     * - `array`：`['job' => 名称, 'data' => 各自负载, 'queue' => 可选]`，
     *   `data` 会与公共 `$data` 合并（条目自身优先）
     *
     * @param  iterable<string|object|Job|array{job?: string|object, name?: string|object, data?: array<string, mixed>, payload?: array<string, mixed>, queue?: string|null}> $jobs
     * @param  array<string, mixed> $data 公共数据（与每个任务合并）
     * @return list<string>
     */
    public function bulk(iterable $jobs, array $data = [], ?string $queue = null): array;

    /**
     * 批量延迟投递（一次性把一批任务排到未来某个时点执行）。
     *
     * 条目形态与 {@see self::bulk()} 完全一致，区别是每个任务都会带上统一的 `$delay`；
     * 数组条目里的 `delay` 键可单独覆盖。适合「整批定时发通知」「错峰重算」这类场景。
     *
     * @param  iterable<string|object|Job|array{job?: string|object, name?: string|object, data?: array<string, mixed>, payload?: array<string, mixed>, queue?: string|null, delay?: int}> $jobs
     * @param  int|DateInterval|DateTimeInterface $delay 统一的延迟（秒 / 时间间隔 / 绝对时间）
     * @param  array<string, mixed> $data 公共数据（与每个任务合并）
     * @return list<string>
     */
    public function bulkLater(
        int|DateInterval|DateTimeInterface $delay,
        iterable $jobs,
        array $data = [],
        ?string $queue = null,
    ): array;

    /**
     * 流式构建任务：`$queue->job('SendMail')->onQueue('mail')->delay(60)->dispatch()`。
     *
     * @param array<string, mixed> $data
     */
    public function job(string|object $job, array $data = []): PendingDispatch;

    /**
     * 拉取并预留一个任务。
     *
     * @param float $timeout 阻塞等待秒数，0 为非阻塞
     */
    public function pop(?string $queue = null, float $timeout = 0.0): ?ReservedJob;

    /**
     * 批量拉取并预留最多 $max 个任务（一次往返取多条，提升高吞吐场景的利用率）。
     *
     * 返回的是已经「预留」的任务句柄，必须像 `pop()` 那样 `ack()`；
     * 未确认的任务在可见性超时后由 Worker 的 `reclaimExpired()` 回收。
     *
     * @param  int   $max     最多返回数量（≤ 0 直接返回空数组）
     * @param  float $timeout 阻塞等待秒数，0 为非阻塞；仅第一个任务会阻塞，其余即时返回
     * @return list<ReservedJob>
     */
    public function popMany(int $max, ?string $queue = null, float $timeout = 0.0): array;

    /**
     * 以生成器方式持续消费，天然适配 `foreach`：
     *
     * ```php
     * foreach ($queue->consume(limit: 100) as $reserved) { ... }
     * ```
     *
     * @param  int|null $limit 最多消费条数，null 表示不限
     * @return Generator<int, ReservedJob>
     */
    public function consume(?string $queue = null, ?int $limit = null, float $timeout = 1.0): Generator;

    /** 确认完成 */
    public function ack(ReservedJob $job): bool;

    /** 放回队列 */
    public function release(ReservedJob $job, int $delay = 0): bool;

    /** 标记失败 */
    public function fail(ReservedJob $job, \Throwable $reason): bool;

    /** 队列长度 */
    public function size(?string $queue = null): int;

    /** 清空队列 */
    public function clear(?string $queue = null): int;

    /** 统计快照 */
    public function stats(?string $queue = null): QueueStats;

    /**
     * 闭包式事务。
     *
     * @template T
     * @param  Closure(self): T $callback
     * @return T
     */
    public function transaction(Closure $callback): mixed;

    /**
     * 返回绑定到指定队列的新实例（不可变，原实例不受影响）。
     */
    public function onQueue(string $queue): static;

    /**
     * 返回追加了中间件的新实例。
     */
    public function withMiddleware(MiddlewareInterface ...$middleware): static;

    /**
     * 返回设置了默认优先级的新实例。
     */
    public function withPriority(Priority $priority): static;

    /** 驱动是否支持某能力 */
    public function supports(Capability $capability): bool;

    /** 取得底层驱动 */
    public function getDriver(): DriverInterface;

    /** 默认队列名 */
    public function getDefaultQueue(): string;

    /** 关闭连接 */
    public function close(): void;
}
