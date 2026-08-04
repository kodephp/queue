<?php

declare(strict_types=1);

namespace Kode\Queue\Message;

use ArrayAccess;
use JsonSerializable;
use Stringable;

/**
 * 已被 Worker 预留（reserved）的任务。
 *
 * 与 {@see Job} 的区别：它额外持有「回执句柄」——各后端确认消息所需的私有标识
 * （Beanstalkd 的 job id、AMQP 的 delivery tag、数据库的自增主键、Redis 的 reserved key……）。
 * 有了它，`ack()` / `release()` / `fail()` 才能精确定位到这一条消息，
 * 而不像 v1 那样只能空实现返回 true。
 *
 * @implements ArrayAccess<string, mixed>
 */
final readonly class ReservedJob implements ArrayAccess, JsonSerializable, Stringable
{
    /**
     * @param Job                  $job       任务本体
     * @param int|string           $receipt   后端回执句柄
     * @param string               $rawPayload 原始负载（release 时可原样写回，避免二次序列化）
     * @param int                  $reservedAt 预留时间戳
     * @param array<string, mixed> $meta       驱动私有元数据
     */
    public function __construct(
        public Job $job,
        public int|string $receipt,
        public string $rawPayload = '',
        public int $reservedAt = 0,
        public array $meta = [],
    ) {
    }

    public static function of(
        Job $job,
        int|string $receipt,
        string $rawPayload = '',
        array $meta = [],
    ): self {
        return new self($job, $receipt, $rawPayload, time(), $meta);
    }

    /** 替换任务本体（例如 attempts+1 后） */
    public function withJob(Job $job): self
    {
        return new self($job, $this->receipt, $this->rawPayload, $this->reservedAt, $this->meta);
    }

    /** 预留是否已超时（超过任务声明的 timeout） */
    public function isExpired(?int $now = null): bool
    {
        if ($this->job->timeout <= 0) {
            return false;
        }

        return ($now ?? time()) - $this->reservedAt > $this->job->timeout;
    }

    /** 已执行时长（秒） */
    public function elapsed(?int $now = null): int
    {
        return max(0, ($now ?? time()) - $this->reservedAt);
    }

    public function jsonSerialize(): array
    {
        return [
            ...$this->job->toArray(),
            'receipt' => $this->receipt,
            'reserved_at' => $this->reservedAt,
            'meta' => $this->meta,
        ];
    }

    public function __toString(): string
    {
        return (string) $this->job;
    }

    // ---- ArrayAccess：让 v1 的 $job['job'] / $job['data'] 写法继续可用 ----

    public function offsetExists(mixed $offset): bool
    {
        return $this->job->offsetExists($offset);
    }

    public function offsetGet(mixed $offset): mixed
    {
        return match ((string) $offset) {
            'receipt' => $this->receipt,
            'reserved_at' => $this->reservedAt,
            'meta' => $this->meta,
            default => $this->job->offsetGet($offset),
        };
    }

    public function offsetSet(mixed $offset, mixed $value): never
    {
        throw new \LogicException('ReservedJob 是不可变对象');
    }

    public function offsetUnset(mixed $offset): never
    {
        throw new \LogicException('ReservedJob 是不可变对象');
    }
}
