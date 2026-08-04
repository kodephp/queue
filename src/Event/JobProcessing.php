<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

use Kode\Queue\Message\ReservedJob;

/**
 * 任务即将开始执行。
 */
final readonly class JobProcessing extends QueueEvent
{
    public function __construct(
        public ReservedJob $reserved,
        public string $worker,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'job.processing',
            'job_id' => $this->reserved->job->id,
            'job_name' => $this->reserved->job->name,
            'queue' => $this->reserved->job->queue,
            'attempts' => $this->reserved->job->attempts,
            'worker' => $this->worker,
            'timestamp' => $this->timestamp,
        ];
    }
}
