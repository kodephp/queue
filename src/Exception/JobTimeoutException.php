<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

use Kode\Queue\Message\Job;

/**
 * 任务执行超时。
 *
 * v1 里 `timeout` 只是个存在负载里、谁也不读的字段——任务卡在某个
 * 没有超时设置的 socket 上，Worker 就跟着一起卡死，队列彻底堵住。
 *
 * v2 由 Worker 用 `pcntl_alarm()` 真正强制执行：超时即抛出本异常，
 * 走正常的重试 / 死信流程，进程继续消费下一条。
 */
final class JobTimeoutException extends QueueException
{
    public static function for(Job $job, int $seconds): self
    {
        return new self(
            sprintf('任务 %s 执行超过 %d 秒被强制中断', (string) $job, $seconds),
            context: [
                'job_id' => $job->id,
                'job_name' => $job->name,
                'queue' => $job->queue,
                'attempts' => $job->attempts,
                'timeout' => $seconds,
            ],
        );
    }

    /** 无法拿到任务上下文时（信号在任务边界之外触发） */
    public static function unknown(int $seconds): self
    {
        return new self(sprintf('任务执行超过 %d 秒被强制中断', $seconds));
    }
}
