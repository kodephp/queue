<?php

declare(strict_types=1);

namespace Kode\Queue\Support;

use JsonSerializable;
use Stringable;

/**
 * Worker 一次运行的结算单（不可变）。
 *
 * 供守护脚本判断退出原因、CI 断言批处理结果、监控采集运行指标。
 */
final readonly class WorkerSummary implements JsonSerializable, Stringable
{
    /** 退出原因：达到 max_jobs / max_time / memory_limit */
    public const string REASON_LIMIT = 'limit';

    /** 退出原因：队列已空且开启了 stop_when_empty */
    public const string REASON_EMPTY = 'empty';

    /** 退出原因：收到停止信号 */
    public const string REASON_SIGNAL = 'signal';

    /** 退出原因：调用方主动 stop() */
    public const string REASON_STOPPED = 'stopped';

    /** 退出原因：致命错误逃逸 */
    public const string REASON_ERROR = 'error';

    public function __construct(
        public private(set) string $worker,
        public string $reason,
        public int $processed = 0,
        public int $failed = 0,
        public int $released = 0,
        public int $reclaimed = 0,
        public private(set) float $uptimeSeconds = 0.0,
        public private(set) int $peakMemoryBytes = 0,
    ) {
    }

    /** 成功处理数（总处理数扣除最终失败数） */
    public function succeeded(): int
    {
        return max(0, $this->processed - $this->failed);
    }

    /** 吞吐（任务/秒） */
    public function throughput(): float
    {
        return $this->uptimeSeconds > 0.0
            ? round($this->processed / $this->uptimeSeconds, 3)
            : 0.0;
    }

    public function peakMemoryMb(): float
    {
        return round($this->peakMemoryBytes / 1048576, 2);
    }

    /** 是否「干净」结束：没有任何最终失败 */
    public function isClean(): bool
    {
        return $this->failed === 0 && $this->reason !== self::REASON_ERROR;
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return [
            'worker' => $this->worker,
            'reason' => $this->reason,
            'processed' => $this->processed,
            'succeeded' => $this->succeeded(),
            'failed' => $this->failed,
            'released' => $this->released,
            'reclaimed' => $this->reclaimed,
            'uptime' => round($this->uptimeSeconds, 3),
            'throughput' => $this->throughput(),
            'peak_memory_mb' => $this->peakMemoryMb(),
        ];
    }

    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    public function __toString(): string
    {
        return sprintf(
            '[%s] %s | 处理 %d（成功 %d / 失败 %d / 重投 %d / 回收 %d）| 运行 %.1fs | 峰值内存 %.1fMB',
            $this->worker,
            $this->reason,
            $this->processed,
            $this->succeeded(),
            $this->failed,
            $this->released,
            $this->reclaimed,
            $this->uptimeSeconds,
            $this->peakMemoryMb(),
        );
    }
}
