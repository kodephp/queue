<?php

declare(strict_types=1);

namespace Kode\Queue\Enum;

/**
 * 任务生命周期状态。
 */
enum JobStatus: string
{
    /** 已入队，等待被拉取 */
    case Pending = 'pending';

    /** 延迟中，未到可执行时间 */
    case Delayed = 'delayed';

    /** 已被 Worker 预留，正在处理 */
    case Reserved = 'reserved';

    /** 处理成功 */
    case Completed = 'completed';

    /** 处理失败，等待重试 */
    case Retrying = 'retrying';

    /** 重试耗尽，进入失败存储 */
    case Failed = 'failed';

    /** 被主动取消 */
    case Cancelled = 'cancelled';

    /** 是否为终态（不会再变化） */
    public function isFinal(): bool
    {
        return match ($this) {
            self::Completed, self::Failed, self::Cancelled => true,
            default => false,
        };
    }

    /** 是否仍占用队列资源 */
    public function isActive(): bool
    {
        return !$this->isFinal();
    }

    public function label(): string
    {
        return match ($this) {
            self::Pending => '等待中',
            self::Delayed => '延迟中',
            self::Reserved => '处理中',
            self::Completed => '已完成',
            self::Retrying => '重试中',
            self::Failed => '已失败',
            self::Cancelled => '已取消',
        };
    }
}
