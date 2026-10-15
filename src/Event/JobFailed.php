<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

use Kode\Queue\Message\ReservedJob;
use Throwable;

/**
 * 任务最终失败（重试已耗尽）。
 */
final readonly class JobFailed extends QueueEvent
{
    public function __construct(
        public ReservedJob $reserved,
        public Throwable $exception,
        public private(set) string $worker,
        public private(set) bool $willRetry = false,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => $this->willRetry ? 'job.retrying' : 'job.failed',
            'job_id' => $this->reserved->job->id,
            'job_name' => $this->reserved->job->name,
            'queue' => $this->reserved->job->queue,
            'attempts' => $this->reserved->job->attempts,
            'max_attempts' => $this->reserved->job->maxAttempts,
            'will_retry' => $this->willRetry,
            'exception' => $this->exception::class,
            'message' => $this->exception->getMessage(),
            'worker' => $this->worker,
            'timestamp' => $this->timestamp,
        ];
    }
}
