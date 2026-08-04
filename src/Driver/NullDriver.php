<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Enum\DriverType;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;
use Override;

/**
 * 空驱动：吞掉一切投递。
 *
 * 适用于压测对照组、灰度关闭某条队列、以及不希望产生副作用的测试用例。
 */
final class NullDriver extends AbstractDriver
{
    private int $discarded = 0;

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Null;
    }

    #[Override]
    public function push(Job $job): string
    {
        $this->discarded++;

        return $job->id;
    }

    #[Override]
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob
    {
        return null;
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        return true;
    }

    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        return true;
    }

    #[Override]
    public function size(string $queue): int
    {
        return 0;
    }

    #[Override]
    public function clear(string $queue): int
    {
        return 0;
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        return QueueStats::of(
            queue: $queue,
            driver: $this->type()->value,
            extra: ['discarded' => $this->discarded],
        );
    }

    #[Override]
    public function isConnected(): bool
    {
        return true;
    }

    #[Override]
    public function close(): void
    {
    }

    /** 已丢弃的任务数 */
    public function discarded(): int
    {
        return $this->discarded;
    }
}
