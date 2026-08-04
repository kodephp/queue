<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

/**
 * Worker 退出。
 */
final readonly class WorkerStopped extends QueueEvent
{
    public function __construct(
        public string $worker,
        public string $reason,
        public int $processed,
        public int $failed,
        public float $uptimeSeconds,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'worker.stopped',
            'worker' => $this->worker,
            'reason' => $this->reason,
            'processed' => $this->processed,
            'failed' => $this->failed,
            'uptime' => round($this->uptimeSeconds, 3),
            'peak_memory_mb' => round(memory_get_peak_usage(true) / 1048576, 2),
            'timestamp' => $this->timestamp,
        ];
    }
}
