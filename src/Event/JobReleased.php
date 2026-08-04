<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

use Kode\Queue\Message\ReservedJob;
use Throwable;

/**
 * 任务处理失败后被重新投回队列等待重试。
 *
 * 与 {@see JobFailed}（`willRetry: true` 时）不同的是：这里是从 Worker 视角派发的
 * 「已重新入队」事件，便于做重试计数、监控告警，而不必去解析 JobFailed 的语义位。
 */
final readonly class JobReleased extends QueueEvent
{
    public function __construct(
        public ReservedJob $reserved,
        public Throwable $exception,
        public string $worker,
        public int $delay,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'job.released',
            'job_id' => $this->reserved->job->id,
            'job_name' => $this->reserved->job->name,
            'queue' => $this->reserved->job->queue,
            'attempts' => $this->reserved->job->attempts,
            'delay' => $this->delay,
            'exception' => $this->exception::class,
            'worker' => $this->worker,
            'timestamp' => $this->timestamp,
        ];
    }
}
