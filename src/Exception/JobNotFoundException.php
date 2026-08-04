<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

/**
 * 找不到任务或任务处理器。
 */
final class JobNotFoundException extends QueueException
{
    public static function job(string $jobId, string $queue): self
    {
        return new self(
            sprintf('队列 "%s" 中不存在任务 %s', $queue, $jobId),
            context: ['job_id' => $jobId, 'queue' => $queue],
        );
    }

    /**
     * @param string|null $reason 具体原因（缺少处理方法、依赖无法注入等）
     */
    public static function handler(string $name, ?string $reason = null): self
    {
        return new self(
            sprintf(
                '无法解析任务处理器 "%s"：%s',
                $name,
                $reason ?? '既不是可调用类，也未在容器/注册表中注册',
            ),
            context: array_filter(['handler' => $name, 'reason' => $reason]),
        );
    }
}
