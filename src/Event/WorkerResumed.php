<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

/**
 * Worker 恢复消费（SIGUSR2 再次切换、SIGCONT，或显式调用 {@see \Kode\Queue\Worker::resume()}）。
 */
final readonly class WorkerResumed extends QueueEvent
{
    public function __construct(
        public private(set) string $worker,
        public ?string $reason = null,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'worker.resumed',
            'worker' => $this->worker,
            'reason' => $this->reason,
            'timestamp' => $this->timestamp,
        ];
    }
}
