<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

/**
 * Worker 暂停消费（SIGUSR2 切换，或显式调用 {@see \Kode\Queue\Worker::pause()}）。
 *
 * 已预留、正在处理的任务会照常跑完，只是不再取新任务。常用于发布期止血：
 * 不重启进程也能先停掉消费，排查完再恢复。
 */
final readonly class WorkerPaused extends QueueEvent
{
    public function __construct(
        public string $worker,
        public ?string $reason = null,
    ) {
        parent::__construct();
    }

    public function toArray(): array
    {
        return [
            'event' => 'worker.paused',
            'worker' => $this->worker,
            'reason' => $this->reason,
            'timestamp' => $this->timestamp,
        ];
    }
}
