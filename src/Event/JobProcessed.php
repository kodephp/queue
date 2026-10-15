<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

use Kode\Queue\Message\ReservedJob;

/**
 * 任务执行成功。
 */
final readonly class JobProcessed extends QueueEvent
{
    public function __construct(
        public ReservedJob $reserved,
        public private(set) string $worker,
        public private(set) float $durationMs,
        public mixed $result = null,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'job.processed',
            'job_id' => $this->reserved->job->id,
            'job_name' => $this->reserved->job->name,
            'queue' => $this->reserved->job->queue,
            'attempts' => $this->reserved->job->attempts,
            'duration_ms' => round($this->durationMs, 3),
            'worker' => $this->worker,
            'timestamp' => $this->timestamp,
        ];
    }
}
