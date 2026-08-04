<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

use Kode\Queue\Config\WorkerOptions;

/**
 * Worker 启动。
 */
final readonly class WorkerStarting extends QueueEvent
{
    public function __construct(
        public WorkerOptions $options,
        public int $pid,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'worker.starting',
            'worker' => $this->options->name,
            'queues' => $this->options->queues,
            'pid' => $this->pid,
            'timestamp' => $this->timestamp,
        ];
    }
}
