<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

use Kode\Queue\Message\Job;

/**
 * 任务已成功投递到队列。
 */
final readonly class JobQueued extends QueueEvent
{
    public function __construct(
        public Job $job,
        public string $connection,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'job.queued',
            'job_id' => $this->job->id,
            'job_name' => $this->job->name,
            'queue' => $this->job->queue,
            'connection' => $this->connection,
            'delay' => $this->job->delaySeconds(),
            'timestamp' => $this->timestamp,
        ];
    }
}
