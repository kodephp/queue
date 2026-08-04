<?php

declare(strict_types=1);

namespace Kode\Queue\Support;

use ArrayAccess;
use JsonSerializable;

/**
 * 队列统计快照（不可变）。
 *
 * v1 的 `stats()` 返回松散数组，字段随驱动而异；v2 统一为该结构，
 * 驱动特有指标放进 `extra`，上层监控（Prometheus / kode/msg）可以稳定取值。
 *
 * @implements ArrayAccess<string, mixed>
 */
final readonly class QueueStats implements ArrayAccess, JsonSerializable
{
    /**
     * @param string               $queue     队列名
     * @param string               $driver    驱动标识
     * @param int                  $ready     可立即消费的任务数
     * @param int                  $delayed   延迟中的任务数
     * @param int                  $reserved  处理中的任务数
     * @param int                  $failed    失败任务数（-1 表示不支持统计）
     * @param int                  $total     总数
     * @param float                $timestamp 采样时刻（含毫秒）
     * @param array<string, mixed> $extra     驱动私有指标
     */
    public function __construct(
        public string $queue,
        public string $driver,
        public int $ready = 0,
        public int $delayed = 0,
        public int $reserved = 0,
        public int $failed = -1,
        public int $total = 0,
        public float $timestamp = 0.0,
        public array $extra = [],
    ) {
    }

    public static function of(
        string $queue,
        string $driver,
        int $ready = 0,
        int $delayed = 0,
        int $reserved = 0,
        int $failed = -1,
        array $extra = [],
    ): self {
        return new self(
            queue: $queue,
            driver: $driver,
            ready: $ready,
            delayed: $delayed,
            reserved: $reserved,
            failed: $failed,
            total: $ready + $delayed + $reserved,
            timestamp: microtime(true),
            extra: $extra,
        );
    }

    /** 队列是否为空 */
    public function isEmpty(): bool
    {
        return $this->total === 0;
    }

    /** 积压率：处理中任务占比，可用于扩缩容判断 */
    public function pressure(): float
    {
        return $this->total > 0 ? round($this->reserved / $this->total, 4) : 0.0;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'queue' => $this->queue,
            'driver' => $this->driver,
            'size' => $this->ready,      // v1 兼容字段
            'ready' => $this->ready,
            'delayed' => $this->delayed,
            'reserved' => $this->reserved,
            'failed' => $this->failed,
            'total' => $this->total,
            'pressure' => $this->pressure(),
            'timestamp' => $this->timestamp,
            ...$this->extra,
        ];
    }

    public function jsonSerialize(): array
    {
        return $this->toArray();
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
        throw new \LogicException('QueueStats 是不可变对象');
    }

    public function offsetUnset(mixed $offset): never
    {
        throw new \LogicException('QueueStats 是不可变对象');
    }
}
