<?php

declare(strict_types=1);

namespace Kode\Queue;

use Closure;
use DateInterval;
use DateTimeInterface;
use Generator;
use Kode\Queue\Attribute\AsJob;
use Kode\Queue\Contract\DriverInterface;
use Kode\Queue\Contract\EventDispatcherInterface;
use Kode\Queue\Contract\MiddlewareInterface;
use Kode\Queue\Contract\QueueInterface;
use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Enum\Capability;
use Kode\Queue\Enum\Priority;
use Kode\Queue\Event\JobQueued;
use Kode\Queue\Event\NullEventDispatcher;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Middleware\Operation;
use Kode\Queue\Middleware\Pipeline;
use Kode\Queue\Support\QueueStats;
use Override;

/**
 * 队列门面。
 *
 * 职责边界很清楚：**它自己不碰任何后端协议**。
 * 驱动负责存取，序列化器负责编解码，中间件负责横切逻辑，
 * 这里只做三件事——把业务参数翻译成 {@see Job}、把调用穿过管道、派发事件。
 *
 * v1 的 `AbstractQueue` 把这些混在一起，导致想给 Redis 加个限流
 * 就得改 `AbstractQueue`，改完所有驱动一起受影响。
 *
 * 实例是不可变的：`onQueue()` / `withMiddleware()` / `withPriority()`
 * 都返回新对象，可以安全地在协程之间共享同一个基础实例。
 */
final class Queue implements QueueInterface
{
    public function __construct(
        private readonly DriverInterface $driver,
        private readonly string $connection = 'default',
        private readonly string $defaultQueue = 'default',
        private readonly Pipeline $pipeline = new Pipeline(),
        private readonly EventDispatcherInterface $events = new NullEventDispatcher(),
        private readonly Priority $defaultPriority = Priority::Normal,
    ) {
    }

    // ------------------------------------------------------------------
    // 生产
    // ------------------------------------------------------------------

    #[Override]
    public function push(string|object $job, array $data = [], ?string $queue = null): string
    {
        return $this->pushJob($this->makeJob($job, $data, $queue));
    }

    #[Override]
    public function pushOn(string $queue, string|object $job, array $data = []): string
    {
        return $this->push($job, $data, $queue);
    }

    #[Override]
    public function later(
        int|DateInterval|DateTimeInterface $delay,
        string|object $job,
        array $data = [],
        ?string $queue = null,
    ): string {
        return $this->pushJob($this->makeJob($job, $data, $queue, $delay));
    }

    #[Override]
    public function pushJob(Job $job): string
    {
        $job = $job->queue !== '' ? $job : $job->withQueue($this->defaultQueue);
        $operation = Operation::of(Operation::PUSH, $job->queue, $this->connection, job: $job);

        /** @var string $id */
        $id = $this->pipeline->run($operation, function (Operation $op): string {
            \assert($op->job instanceof Job);
            $id = $this->driver->push($op->job);
            $this->events->dispatch(new JobQueued($op->job, $this->connection));

            return $id;
        });

        return $id;
    }

    #[Override]
    public function bulk(iterable $jobs, array $data = [], ?string $queue = null): array
    {
        $prepared = [];

        foreach ($jobs as $job) {
            $prepared[] = match (true) {
                // 已经是 Job：只在给了公共 data 时合并
                $job instanceof Job => $data === [] ? $job : $job->mergePayload($data),
                // ['job' => 名称或类名, 'data' => 各自的负载, 'queue' => 可选]
                \is_array($job) => $this->makeJob(
                    $job['job'] ?? $job['name'] ?? throw new \InvalidArgumentException(
                        'bulk() 的数组条目必须包含 "job"（或 "name"）键',
                    ),
                    [...$data, ...(array) ($job['data'] ?? $job['payload'] ?? [])],
                    $job['queue'] ?? $queue,
                ),
                default => $this->makeJob($job, $data, $queue),
            };
        }

        if ($prepared === []) {
            return [];
        }

        $operation = Operation::of(
            Operation::PUSH_BATCH,
            $queue ?? $this->defaultQueue,
            $this->connection,
            arguments: [$prepared],
            job: $prepared[0],
        );

        /** @var list<string> $ids */
        $ids = $this->pipeline->run($operation, function (Operation $op) use ($prepared): array {
            $ids = $this->driver->pushBatch($prepared);

            foreach ($prepared as $job) {
                $this->events->dispatch(new JobQueued($job, $this->connection));
            }

            return $ids;
        });

        return $ids;
    }

    #[Override]
    public function bulkLater(
        int|DateInterval|DateTimeInterface $delay,
        iterable $jobs,
        array $data = [],
        ?string $queue = null,
    ): array {
        $prepared = [];

        foreach ($jobs as $job) {
            $prepared[] = match (true) {
                // 已经是 Job：自带延迟优先；与 bulk() 保持一致，不强行覆盖对象上的延迟
                $job instanceof Job => $data === [] ? $job : $job->mergePayload($data),
                // 数组条目可单独用 'delay' 键覆盖统一延迟
                \is_array($job) => $this->makeJob(
                    $job['job'] ?? $job['name'] ?? throw new \InvalidArgumentException(
                        'bulkLater() 的数组条目必须包含 "job"（或 "name"）键',
                    ),
                    [...$data, ...(array) ($job['data'] ?? $job['payload'] ?? [])],
                    $job['queue'] ?? $queue,
                    $job['delay'] ?? $delay,
                ),
                default => $this->makeJob($job, $data, $queue, $delay),
            };
        }

        if ($prepared === []) {
            return [];
        }

        $operation = Operation::of(
            Operation::PUSH_DELAYED_BATCH,
            $queue ?? $this->defaultQueue,
            $this->connection,
            arguments: [$prepared, $delay],
            job: $prepared[0],
        );

        /** @var list<string> $ids */
        $ids = $this->pipeline->run($operation, function (Operation $op) use ($prepared): array {
            $ids = $this->driver->pushBatch($prepared);

            foreach ($prepared as $job) {
                $this->events->dispatch(new JobQueued($job, $this->connection));
            }

            return $ids;
        });

        return $ids;
    }

    #[Override]
    public function job(string|object $job, array $data = []): PendingDispatch
    {
        return (new PendingDispatch($this, $this->resolveName($job), $this->resolvePayload($job, $data)))
            ->onQueue($this->resolveQueue($job) ?? $this->defaultQueue)
            ->priority($this->resolvePriority($job));
    }

    // ------------------------------------------------------------------
    // 消费
    // ------------------------------------------------------------------

    #[Override]
    public function pop(?string $queue = null, float $timeout = 0.0): ?ReservedJob
    {
        $target = $queue ?? $this->defaultQueue;
        $operation = Operation::of(Operation::POP, $target, $this->connection, [$target, $timeout]);

        /** @var ReservedJob|null */
        return $this->pipeline->run(
            $operation,
            fn (Operation $op): ?ReservedJob => $this->driver->pop($target, $timeout),
        );
    }

    #[Override]
    public function consume(?string $queue = null, ?int $limit = null, float $timeout = 1.0): Generator
    {
        $consumed = 0;

        while ($limit === null || $consumed < $limit) {
            $reserved = $this->pop($queue, $timeout);

            if ($reserved === null) {
                // 非阻塞模式下队列已空，交还控制权而不是空转
                if ($timeout <= 0.0) {
                    return;
                }

                continue;
            }

            $consumed++;

            yield $reserved;
        }
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        return $this->through(Operation::ACK, $job, fn (): bool => $this->driver->ack($job));
    }

    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        return $this->through(
            Operation::RELEASE,
            $job,
            fn (): bool => $this->driver->release($job, $delay),
        );
    }

    #[Override]
    public function fail(ReservedJob $job, \Throwable $reason): bool
    {
        return $this->through(
            Operation::FAIL,
            $job,
            fn (): bool => $this->driver->fail($job, $reason),
        );
    }

    /**
     * 窥视队首任务（不消费）。
     */
    public function peek(?string $queue = null): ?Job
    {
        $target = $queue ?? $this->defaultQueue;

        /** @var Job|null */
        return $this->pipeline->run(
            Operation::of(Operation::PEEK, $target, $this->connection, [$target]),
            fn (): ?Job => $this->driver->peek($target),
        );
    }

    // ------------------------------------------------------------------
    // 管理
    // ------------------------------------------------------------------

    #[Override]
    public function size(?string $queue = null): int
    {
        $target = $queue ?? $this->defaultQueue;

        /** @var int */
        return $this->pipeline->run(
            Operation::of(Operation::SIZE, $target, $this->connection, [$target]),
            fn (): int => $this->driver->size($target),
        );
    }

    #[Override]
    public function clear(?string $queue = null): int
    {
        $target = $queue ?? $this->defaultQueue;

        /** @var int */
        return $this->pipeline->run(
            Operation::of(Operation::CLEAR, $target, $this->connection, [$target]),
            fn (): int => $this->driver->clear($target),
        );
    }

    #[Override]
    public function stats(?string $queue = null): QueueStats
    {
        $target = $queue ?? $this->defaultQueue;

        /** @var QueueStats */
        return $this->pipeline->run(
            Operation::of(Operation::STATS, $target, $this->connection, [$target]),
            fn (): QueueStats => $this->driver->stats($target),
        );
    }

    /**
     * 回收超时未确认的任务。
     */
    public function reclaimExpired(?string $queue = null): int
    {
        return $this->driver->reclaimExpired($queue ?? $this->defaultQueue);
    }

    #[Override]
    public function transaction(Closure $callback): mixed
    {
        return $this->driver->transaction(fn (): mixed => $callback($this));
    }

    // ------------------------------------------------------------------
    // 不可变变更
    // ------------------------------------------------------------------

    #[Override]
    public function onQueue(string $queue): static
    {
        return $queue === $this->defaultQueue ? $this : $this->derive(queue: $queue);
    }

    #[Override]
    public function withMiddleware(MiddlewareInterface ...$middleware): static
    {
        return $middleware === [] ? $this : $this->derive(pipeline: $this->pipeline->with(...$middleware));
    }

    /**
     * 移除指定中间件（临时绕过限流 / 幂等等场景）。
     *
     * @param class-string ...$classes
     */
    public function withoutMiddleware(string ...$classes): static
    {
        return $this->derive(pipeline: $this->pipeline->without(...$classes));
    }

    #[Override]
    public function withPriority(Priority $priority): static
    {
        return $this->derive(priority: $priority);
    }

    /**
     * 更换事件派发器（接入 kode/event 或任意 PSR-14 实现）。
     */
    public function withEvents(EventDispatcherInterface $events): static
    {
        return $this->derive(events: $events);
    }

    // ------------------------------------------------------------------
    // 元信息
    // ------------------------------------------------------------------

    #[Override]
    public function supports(Capability $capability): bool
    {
        return $this->driver->supports($capability);
    }

    #[Override]
    public function getDriver(): DriverInterface
    {
        return $this->driver;
    }

    #[Override]
    public function getDefaultQueue(): string
    {
        return $this->defaultQueue;
    }

    public function getConnectionName(): string
    {
        return $this->connection;
    }

    public function getPipeline(): Pipeline
    {
        return $this->pipeline;
    }

    public function getEvents(): EventDispatcherInterface
    {
        return $this->events;
    }

    #[Override]
    public function close(): void
    {
        $this->driver->close();
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /**
     * 把「任务名 / 任务对象 + 数据」翻译成 {@see Job}，
     * 期间读取 `#[AsJob]` 上声明的默认参数。
     *
     * @param array<string, mixed> $data
     */
    private function makeJob(
        string|object $job,
        array $data,
        ?string $queue = null,
        int|DateInterval|DateTimeInterface|null $delay = null,
    ): Job {
        if ($job instanceof Job) {
            $merged = $data === [] ? $job : $job->mergePayload($data);

            return $queue === null ? $merged : $merged->withQueue($queue);
        }

        $attribute = AsJob::of($job);

        return Job::create(
            name: $this->resolveName($job),
            payload: $this->resolvePayload($job, $data),
            queue: $queue ?? $attribute?->queue ?? $this->defaultQueue,
            delay: $delay,
            maxAttempts: $attribute?->maxAttempts,
            priority: $attribute?->priority ?? $this->defaultPriority,
            timeout: $attribute?->timeout,
            backoff: $attribute?->backoff ?? BackoffStrategy::ExponentialJitter,
            backoffBase: $attribute?->backoffBase ?? 5,
            headers: $attribute?->headers ?? [],
        );
    }

    private function resolveName(string|object $job): string
    {
        return match (true) {
            $job instanceof Job => $job->name,
            \is_string($job) => AsJob::of($job)?->name ?? $job,
            default => AsJob::nameOf($job),
        };
    }

    /**
     * 任务对象自带状态时，把它的公开属性并入 payload，
     * 这样 `new SendMail(userId: 42)` 也能直接投递。
     *
     * @param  array<string, mixed> $data
     * @return array<string, mixed>
     */
    private function resolvePayload(string|object $job, array $data): array
    {
        if (\is_string($job)) {
            return $data;
        }

        if ($job instanceof Job) {
            return [...$job->payload, ...$data];
        }

        return [...get_object_vars($job), ...$data];
    }

    private function resolveQueue(string|object $job): ?string
    {
        return AsJob::of($job)?->queue;
    }

    private function resolvePriority(string|object $job): Priority
    {
        return AsJob::of($job)?->priority ?? $this->defaultPriority;
    }

    /**
     * ack / release / fail 的公共穿管逻辑。
     *
     * @param Closure(): bool $destination
     */
    private function through(string $name, ReservedJob $job, Closure $destination): bool
    {
        /** @var bool */
        return $this->pipeline->run(
            Operation::of($name, $job->job->queue, $this->connection, reserved: $job, job: $job->job),
            static fn (): bool => $destination(),
        );
    }

    private function derive(
        ?string $queue = null,
        ?Pipeline $pipeline = null,
        ?EventDispatcherInterface $events = null,
        ?Priority $priority = null,
    ): self {
        return new self(
            driver: $this->driver,
            connection: $this->connection,
            defaultQueue: $queue ?? $this->defaultQueue,
            pipeline: $pipeline ?? $this->pipeline,
            events: $events ?? $this->events,
            defaultPriority: $priority ?? $this->defaultPriority,
        );
    }
}
