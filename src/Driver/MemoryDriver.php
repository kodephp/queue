<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Enum\DriverType;
use Kode\Queue\Enum\JobStatus;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;
use Override;
use SplPriorityQueue;

/**
 * 进程内内存驱动。
 *
 * 用途：单元测试、CI、单进程脚本、以及「先跑起来再说」的本地开发。
 * v1 的 SyncDriver 用普通数组模拟，既没有优先级也没有预留语义，
 * 导致「测试通过、上 Redis 就挂」。这里改为与真实驱动完全一致的语义：
 * 优先级排序、可见性超时、ack / release / reclaim 一应俱全。
 */
class MemoryDriver extends AbstractDriver
{
    /** @var array<string, SplPriorityQueue<array{int, int}, Job>> */
    protected array $queues = [];

    /** @var array<string, array<string, Job>> 延迟任务 queue => [jobId => Job] */
    protected array $delayed = [];

    /** @var array<string, array{job: Job, reservedAt: int}> 预留中的任务 */
    protected array $reserved = [];

    /** 自增序列，保证同优先级下 FIFO */
    private int $sequence = PHP_INT_MAX;

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Memory;
    }

    #[Override]
    public function push(Job $job): string
    {
        $job = $this->normalize($job);
        $this->connected = true;

        if (!$job->isReady()) {
            $this->delayed[$job->queue][$job->id] = $job;

            return $job->id;
        }

        $this->enqueue($job);

        return $job->id;
    }

    #[Override]
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob
    {
        $deadline = microtime(true) + max(0.0, $timeout);

        do {
            $this->migrateDelayed($queue);
            $this->reclaimExpired($queue);

            $bucket = $this->queues[$queue] ?? null;
            if ($bucket !== null && !$bucket->isEmpty()) {
                /** @var Job $job */
                $job = $bucket->extract();
                $reserved = $job->incrementAttempts();
                $this->reserved[$job->id] = ['job' => $reserved, 'reservedAt' => time()];

                return ReservedJob::of($reserved, $job->id, $this->encode($reserved));
            }

            if ($timeout > 0.0 && microtime(true) < $deadline) {
                usleep(10_000);
            }
        } while ($timeout > 0.0 && microtime(true) < $deadline);

        return null;
    }

    #[Override]
    public function peek(string $queue): ?Job
    {
        $this->migrateDelayed($queue);
        $bucket = $this->queues[$queue] ?? null;

        if ($bucket === null || $bucket->isEmpty()) {
            return null;
        }

        // SplPriorityQueue 无法只读取，复制一份再取
        $copy = clone $bucket;

        /** @var Job */
        return $copy->extract();
    }

    #[Override]
    public function popMany(string $queue, int $max, float $timeout = 0.0): array
    {
        $deadline = microtime(true) + max(0.0, $timeout);
        $reserved = [];

        do {
            $this->migrateDelayed($queue);
            $this->reclaimExpired($queue);

            $bucket = $this->queues[$queue] ?? null;

            // 一次性把当前就绪的任务按「优先级 + FIFO」取出最多 $max 条
            while ($bucket !== null && !$bucket->isEmpty() && \count($reserved) < $max) {
                /** @var Job $job */
                $job = $bucket->extract();
                $reservedJob = $job->incrementAttempts();
                $this->reserved[$reservedJob->id] = ['job' => $reservedJob, 'reservedAt' => time()];
                $reserved[] = ReservedJob::of($reservedJob, $reservedJob->id, $this->encode($reservedJob));
            }

            if (\count($reserved) >= $max) {
                return $reserved;
            }

            // 还没凑满且允许阻塞：空轮询直到有任务或超时
            if ($timeout > 0.0 && microtime(true) < $deadline) {
                usleep(10_000);
            }
        } while ($timeout > 0.0 && microtime(true) < $deadline);

        return $reserved;
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        unset($this->reserved[(string) $job->receipt]);

        return true;
    }

    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        unset($this->reserved[(string) $job->receipt]);
        $this->push($job->job->delayedBy($delay)->withStatus(
            $delay > 0 ? JobStatus::Delayed : JobStatus::Pending,
        ));

        return true;
    }

    #[Override]
    public function size(string $queue): int
    {
        $this->migrateDelayed($queue);

        return \count($this->queues[$queue] ?? []);
    }

    #[Override]
    public function clear(string $queue): int
    {
        $count = \count($this->queues[$queue] ?? []) + \count($this->delayed[$queue] ?? []);

        unset($this->queues[$queue], $this->delayed[$queue]);

        foreach ($this->reserved as $id => $entry) {
            if ($entry['job']->queue === $queue) {
                unset($this->reserved[$id]);
                $count++;
            }
        }

        return $count;
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        $this->migrateDelayed($queue);

        $reserved = 0;
        foreach ($this->reserved as $entry) {
            if ($entry['job']->queue === $queue) {
                $reserved++;
            }
        }

        return QueueStats::of(
            queue: $queue,
            driver: $this->type()->value,
            ready: \count($this->queues[$queue] ?? []),
            delayed: \count($this->delayed[$queue] ?? []),
            reserved: $reserved,
        );
    }

    #[Override]
    public function reclaimExpired(string $queue): int
    {
        $now = time();
        $retryAfter = $this->retryAfter();
        $recovered = 0;

        foreach ($this->reserved as $id => $entry) {
            if ($entry['job']->queue !== $queue) {
                continue;
            }

            if ($now - $entry['reservedAt'] >= $retryAfter) {
                unset($this->reserved[$id]);
                $this->enqueue($entry['job']->withStatus(JobStatus::Pending));
                $recovered++;
            }
        }

        return $recovered;
    }

    #[Override]
    public function close(): void
    {
        $this->queues = [];
        $this->delayed = [];
        $this->reserved = [];
        $this->connected = false;
    }

    #[Override]
    public function isConnected(): bool
    {
        return true;
    }

    /**
     * 当前全部待处理任务（测试断言用）。
     *
     * @return list<Job>
     */
    public function all(string $queue): array
    {
        $this->migrateDelayed($queue);
        $bucket = $this->queues[$queue] ?? null;

        if ($bucket === null) {
            return [];
        }

        $copy = clone $bucket;
        $out = [];
        while (!$copy->isEmpty()) {
            /** @var Job $job */
            $job = $copy->extract();
            $out[] = $job;
        }

        return $out;
    }

    /**
     * 入队并按「优先级降序 + 到达顺序」排序。
     */
    protected function enqueue(Job $job): void
    {
        $bucket = $this->queues[$job->queue] ??= new SplPriorityQueue();
        $bucket->setExtractFlags(SplPriorityQueue::EXTR_DATA);
        // 优先级相同时用递减序列号保证 FIFO
        $bucket->insert($job, [$job->priority->value, $this->sequence--]);
    }

    /**
     * 把到期的延迟任务搬进主队列。
     */
    protected function migrateDelayed(string $queue): void
    {
        $now = time();

        foreach ($this->delayed[$queue] ?? [] as $id => $job) {
            if ($job->isReady($now)) {
                unset($this->delayed[$queue][$id]);
                $this->enqueue($job->withStatus(JobStatus::Pending));
            }
        }
    }
}
