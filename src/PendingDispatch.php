<?php

declare(strict_types=1);

namespace Kode\Queue;

use DateInterval;
use DateTimeInterface;
use Kode\Queue\Contract\QueueInterface;
use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Enum\Priority;
use Kode\Queue\Message\Job;

/**
 * 流式任务构建器。
 *
 * v1 的投递签名是 `push($job, $data, $queue, $delay, $attempts, $priority, ...)`，
 * 参数一多就变成「靠数位置传 null」的猜谜游戏。这里改成链式声明，
 * 每个参数都有名字，读代码时一眼看懂：
 *
 * ```php
 * $queue->job(SendInvoice::class, ['order' => 1024])
 *       ->onQueue('billing')
 *       ->priority(Priority::High)
 *       ->delay(new DateInterval('PT30S'))
 *       ->retries(5, BackoffStrategy::ExponentialJitter, base: 10)
 *       ->withHeaders(['tenant' => 'acme'])
 *       ->dispatch();
 * ```
 *
 * 本对象是可变的（builder 模式），但 `dispatch()` 产出的 {@see Job} 不可变。
 */
final class PendingDispatch
{
    private ?string $queue = null;

    private int|DateInterval|DateTimeInterface|null $delay = null;

    private ?int $maxAttempts = null;

    private ?Priority $priority = null;

    private ?int $timeout = null;

    private BackoffStrategy $backoff = BackoffStrategy::ExponentialJitter;

    private int $backoffBase = 5;

    /** @var array<string, mixed> */
    private array $headers = [];

    /** @var array<string, mixed> */
    private array $payload;

    private bool $dispatched = false;

    /**
     * @param array<string, mixed> $payload
     */
    public function __construct(
        private readonly QueueInterface $connection,
        private readonly string $name,
        array $payload = [],
    ) {
        $this->payload = $payload;
    }

    // ------------------------------------------------------------------
    // 构建
    // ------------------------------------------------------------------

    public function onQueue(string $queue): self
    {
        $this->queue = $queue;

        return $this;
    }

    /**
     * @param array<string, mixed> $payload
     */
    public function with(array $payload): self
    {
        $this->payload = [...$this->payload, ...$payload];

        return $this;
    }

    public function delay(int|DateInterval|DateTimeInterface $delay): self
    {
        $this->delay = $delay;

        return $this;
    }

    /**
     * 指定绝对执行时间。
     */
    public function at(DateTimeInterface $moment): self
    {
        $this->delay = $moment;

        return $this;
    }

    public function priority(Priority $priority): self
    {
        $this->priority = $priority;

        return $this;
    }

    /**
     * 重试策略。
     *
     * @param int $attempts 最大尝试次数，<= 0 表示无限重试
     */
    public function retries(
        int $attempts,
        BackoffStrategy $backoff = BackoffStrategy::ExponentialJitter,
        int $base = 5,
    ): self {
        $this->maxAttempts = $attempts;
        $this->backoff = $backoff;
        $this->backoffBase = max(1, $base);

        return $this;
    }

    public function timeout(int $seconds): self
    {
        $this->timeout = max(0, $seconds);

        return $this;
    }

    /**
     * @param array<string, mixed> $headers
     */
    public function withHeaders(array $headers): self
    {
        $this->headers = [...$this->headers, ...$headers];

        return $this;
    }

    /**
     * 链路追踪 ID（不传则由 ContextMiddleware 自动注入）。
     */
    public function traceId(string $traceId): self
    {
        $this->headers['trace_id'] = $traceId;

        return $this;
    }

    // ------------------------------------------------------------------
    // 产出
    // ------------------------------------------------------------------

    /**
     * 构建但不投递（用于批量投递或单元测试断言）。
     */
    public function toJob(): Job
    {
        return Job::create(
            name: $this->name,
            payload: $this->payload,
            queue: $this->queue ?? $this->connection->getDefaultQueue(),
            delay: $this->delay,
            maxAttempts: $this->maxAttempts,
            priority: $this->priority ?? Priority::Normal,
            timeout: $this->timeout,
            backoff: $this->backoff,
            backoffBase: $this->backoffBase,
            headers: $this->headers,
        );
    }

    /**
     * 投递，返回任务 ID。
     */
    public function dispatch(): string
    {
        $this->dispatched = true;

        return $this->connection->pushJob($this->toJob());
    }

    /**
     * 条件投递：不满足时返回 null，不产生任何副作用。
     */
    public function dispatchIf(bool $condition): ?string
    {
        return $condition ? $this->dispatch() : null;
    }

    public function dispatchUnless(bool $condition): ?string
    {
        return $this->dispatchIf(!$condition);
    }

    /**
     * 是否已经投递过（避免重复 dispatch）。
     */
    public function isDispatched(): bool
    {
        return $this->dispatched;
    }
}
