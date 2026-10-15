<?php

declare(strict_types=1);

namespace Kode\Queue\Message;

use ArrayAccess;
use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use JsonSerializable;
use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Enum\JobStatus;
use Kode\Queue\Enum\Priority;
use Kode\Queue\Support\IdGenerator;
use Stringable;

/**
 * 任务消息对象（不可变）。
 *
 * 旧版用裸数组 + json_encode 在各驱动之间传递负载，字段散落、拼写错误无法被发现。
 * v2 统一收敛为这个 readonly 值对象：
 *
 * - `readonly class`（PHP 8.2）保证全字段不可变，天然协程 / 多线程安全；
 * - 变更一律通过 `withXxx()` 返回新实例（withers 模式），杜绝共享状态被意外改写；
 * - 类型化类常量（PHP 8.3）声明 schema 版本与默认值，配置错误在编译期即被发现；
 * - 实现 `ArrayAccess`，让 v1 时代 `$job['job']` / `$job['data']` 的写法继续可用。
 *
 * @implements ArrayAccess<string, mixed>
 */
final readonly class Job implements ArrayAccess, JsonSerializable, Stringable
{
    /** 负载格式版本号，用于未来的平滑升级 */
    public const int SCHEMA_VERSION = 2;

    /** 默认最大尝试次数 */
    public const int DEFAULT_MAX_ATTEMPTS = 3;

    /** 默认执行超时（秒），0 表示不限制 */
    public const int DEFAULT_TIMEOUT = 60;

    /**
     * @param string               $id           全局唯一任务 ID（ULID，天然按时间有序）
     * @param string               $name         任务名：类名、别名或 handler 标识
     * @param array<string, mixed> $payload      业务数据
     * @param string               $queue        所属队列
     * @param int                  $attempts     已尝试次数
     * @param int                  $maxAttempts  最大尝试次数
     * @param Priority             $priority     优先级
     * @param int                  $timeout      单次执行超时（秒）
     * @param BackoffStrategy      $backoff      重试退避策略
     * @param int                  $backoffBase  退避基础间隔（秒）
     * @param int                  $availableAt  可执行时间戳（延迟任务 > 当前时间）
     * @param int                  $createdAt    创建时间戳
     * @param JobStatus            $status       状态
     * @param array<string, mixed> $headers      链路 / 租户 / 追踪等元数据
     */
    public function __construct(
        public string $id,
        public string $name,
        public array $payload = [],
        public string $queue = 'default',
        public int $attempts = 0,
        public int $maxAttempts = self::DEFAULT_MAX_ATTEMPTS,
        public Priority $priority = Priority::Normal,
        public int $timeout = self::DEFAULT_TIMEOUT,
        public BackoffStrategy $backoff = BackoffStrategy::ExponentialJitter,
        public int $backoffBase = 5,
        public private(set) int $availableAt = 0,
        public int $createdAt = 0,
        public JobStatus $status = JobStatus::Pending,
        public array $headers = [],
    ) {
    }

    /**
     * 创建一个新任务（自动生成 ID 与时间戳）。
     *
     * @param array<string, mixed> $payload
     * @param array<string, mixed> $headers
     */
    public static function create(
        string $name,
        array $payload = [],
        string $queue = 'default',
        int|DateInterval|DateTimeInterface|null $delay = null,
        ?int $maxAttempts = null,
        Priority $priority = Priority::Normal,
        ?int $timeout = null,
        BackoffStrategy $backoff = BackoffStrategy::ExponentialJitter,
        int $backoffBase = 5,
        array $headers = [],
    ): self {
        $now = time();
        $delaySeconds = self::normalizeDelay($delay, $now);

        return new self(
            id: IdGenerator::ulid(),
            name: $name,
            payload: $payload,
            queue: $queue,
            maxAttempts: $maxAttempts ?? self::DEFAULT_MAX_ATTEMPTS,
            priority: $priority,
            timeout: $timeout ?? self::DEFAULT_TIMEOUT,
            backoff: $backoff,
            backoffBase: $backoffBase,
            availableAt: $now + $delaySeconds,
            createdAt: $now,
            status: $delaySeconds > 0 ? JobStatus::Delayed : JobStatus::Pending,
            headers: $headers,
        );
    }

    /**
     * 从数组还原（驱动反序列化后调用）。兼容 v1 的 `job`/`data` 字段名。
     *
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $createdAt = (int) ($data['created_at'] ?? time());

        return new self(
            id: (string) ($data['id'] ?? IdGenerator::ulid()),
            // v2 用 name，v1 用 job
            name: (string) ($data['name'] ?? $data['job'] ?? ''),
            // v2 用 payload，v1 用 data
            payload: (array) ($data['payload'] ?? $data['data'] ?? []),
            queue: (string) ($data['queue'] ?? 'default'),
            attempts: (int) ($data['attempts'] ?? 0),
            maxAttempts: (int) ($data['max_attempts'] ?? self::DEFAULT_MAX_ATTEMPTS),
            priority: self::resolvePriority($data['priority'] ?? null),
            timeout: (int) ($data['timeout'] ?? self::DEFAULT_TIMEOUT),
            backoff: BackoffStrategy::tryFrom((string) ($data['backoff'] ?? ''))
                ?? BackoffStrategy::ExponentialJitter,
            backoffBase: (int) ($data['backoff_base'] ?? 5),
            availableAt: (int) ($data['available_at'] ?? $createdAt),
            createdAt: $createdAt,
            status: JobStatus::tryFrom((string) ($data['status'] ?? '')) ?? JobStatus::Pending,
            headers: (array) ($data['headers'] ?? []),
        );
    }

    /**
     * 序列化为数组。同时输出 v1 的 `job`/`data` 别名字段，保证旧消费者可读。
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'v' => self::SCHEMA_VERSION,
            'id' => $this->id,
            'name' => $this->name,
            'payload' => $this->payload,
            'queue' => $this->queue,
            'attempts' => $this->attempts,
            'max_attempts' => $this->maxAttempts,
            'priority' => $this->priority->value,
            'timeout' => $this->timeout,
            'backoff' => $this->backoff->value,
            'backoff_base' => $this->backoffBase,
            'available_at' => $this->availableAt,
            'created_at' => $this->createdAt,
            'status' => $this->status->value,
            'headers' => $this->headers,
            // ↓ v1 兼容别名
            'job' => $this->name,
            'data' => $this->payload,
        ];
    }

    // ------------------------------------------------------------------
    // 不可变变更（借助 PHP 8.3 的 readonly + __clone 重初始化）
    // ------------------------------------------------------------------

    public function withQueue(string $queue): self
    {
        return $this->mutate(queue: $queue);
    }

    public function withPayload(array $payload): self
    {
        return $this->mutate(payload: $payload);
    }

    /** 合并（而非替换）业务数据 */
    public function mergePayload(array $payload): self
    {
        return $this->mutate(payload: [...$this->payload, ...$payload]);
    }

    public function withHeaders(array $headers): self
    {
        return $this->mutate(headers: [...$this->headers, ...$headers]);
    }

    public function withStatus(JobStatus $status): self
    {
        return $this->mutate(status: $status);
    }

    public function withPriority(Priority $priority): self
    {
        return $this->mutate(priority: $priority);
    }

    /** 尝试次数 +1，并置为处理中 */
    public function incrementAttempts(): self
    {
        return $this->mutate(attempts: $this->attempts + 1, status: JobStatus::Reserved);
    }

    /** 尝试次数清零（人工重放死信时使用，否则一进队列即判定「已耗尽」） */
    public function resetAttempts(): self
    {
        return $this->mutate(attempts: 0);
    }

    /** 延后到 N 秒之后可执行 */
    public function delayedBy(int $seconds): self
    {
        return $this->mutate(
            availableAt: time() + max(0, $seconds),
            status: $seconds > 0 ? JobStatus::Delayed : JobStatus::Pending,
        );
    }

    // ------------------------------------------------------------------
    // 查询
    // ------------------------------------------------------------------

    /** 是否已到可执行时间 */
    public function isReady(?int $now = null): bool
    {
        return $this->availableAt <= ($now ?? time());
    }

    /** 是否还能继续重试 */
    public function canRetry(): bool
    {
        return $this->maxAttempts <= 0 || $this->attempts < $this->maxAttempts;
    }

    /** 剩余可重试次数（-1 表示无限） */
    public function remainingAttempts(): int
    {
        return $this->maxAttempts <= 0 ? -1 : max(0, $this->maxAttempts - $this->attempts);
    }

    /** 下次重试应等待的秒数 */
    public function nextRetryDelay(): int
    {
        return $this->backoff->delayFor(max(1, $this->attempts), $this->backoffBase);
    }

    /** 剩余延迟秒数 */
    public function delaySeconds(?int $now = null): int
    {
        return max(0, $this->availableAt - ($now ?? time()));
    }

    /** 链路追踪 ID（若上游注入） */
    public function traceId(): ?string
    {
        $value = $this->headers['trace_id'] ?? null;

        return \is_string($value) ? $value : null;
    }

    public function createdAt(): DateTimeImmutable
    {
        return (new DateTimeImmutable())->setTimestamp($this->createdAt);
    }

    public function availableAt(): DateTimeImmutable
    {
        return (new DateTimeImmutable())->setTimestamp($this->availableAt);
    }

    // ------------------------------------------------------------------
    // 接口实现
    // ------------------------------------------------------------------

    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    public function __toString(): string
    {
        return sprintf('%s#%s@%s', $this->name, $this->id, $this->queue);
    }

    public function offsetExists(mixed $offset): bool
    {
        return \array_key_exists((string) $offset, $this->toArray());
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->toArray()[(string) $offset] ?? null;
    }

    public function offsetSet(mixed $offset, mixed $value): never
    {
        throw new \LogicException('Job 是不可变对象，请使用 withXxx() 生成新实例');
    }

    public function offsetUnset(mixed $offset): never
    {
        throw new \LogicException('Job 是不可变对象，请使用 withXxx() 生成新实例');
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /**
     * 统一的不可变变更入口：只在一处罗列全部构造参数，
     * 新增字段时无需逐个修改所有 withXxx() 方法。
     */
    private function mutate(
        ?array $payload = null,
        ?string $queue = null,
        ?int $attempts = null,
        ?Priority $priority = null,
        ?int $availableAt = null,
        ?JobStatus $status = null,
        ?array $headers = null,
    ): self {
        $overrides = array_filter(
            [
                'payload' => $payload,
                'queue' => $queue,
                'attempts' => $attempts,
                'priority' => $priority,
                'availableAt' => $availableAt,
                'status' => $status,
                'headers' => $headers,
            ],
            static fn (mixed $v): bool => $v !== null,
        );

        return new self(
            id: $this->id,
            name: $this->name,
            payload: $overrides['payload'] ?? $this->payload,
            queue: $overrides['queue'] ?? $this->queue,
            attempts: $overrides['attempts'] ?? $this->attempts,
            maxAttempts: $this->maxAttempts,
            priority: $overrides['priority'] ?? $this->priority,
            timeout: $this->timeout,
            backoff: $this->backoff,
            backoffBase: $this->backoffBase,
            availableAt: $overrides['availableAt'] ?? $this->availableAt,
            createdAt: $this->createdAt,
            status: $overrides['status'] ?? $this->status,
            headers: $overrides['headers'] ?? $this->headers,
        );
    }

    private static function normalizeDelay(
        int|DateInterval|DateTimeInterface|null $delay,
        int $now,
    ): int {
        return match (true) {
            $delay === null => 0,
            \is_int($delay) => max(0, $delay),
            $delay instanceof DateTimeInterface => max(0, $delay->getTimestamp() - $now),
            $delay instanceof DateInterval => max(
                0,
                (new DateTimeImmutable('@' . $now))->add($delay)->getTimestamp() - $now,
            ),
        };
    }

    private static function resolvePriority(mixed $value): Priority
    {
        return match (true) {
            $value instanceof Priority => $value,
            \is_int($value) => Priority::tryFrom($value) ?? Priority::fromScore($value),
            \is_string($value) && is_numeric($value) => Priority::fromScore((int) $value),
            default => Priority::Normal,
        };
    }
}
