<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Override;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use Throwable;

/**
 * 结构化日志中间件。
 *
 * v1 直接 `echo` 到标准输出，既无法分级也无法接入 ELK。
 * v2 依赖 PSR-3，任何日志库（Monolog、kode/exception 的 LoggerFactory）都能接。
 * 日志以结构化上下文输出，字段稳定、可检索。
 */
final class LoggingMiddleware extends AbstractMiddleware
{
    /** 默认优先级：尽量靠外，覆盖内层中间件的耗时 */
    public const int PRIORITY = 900;

    public function __construct(
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly float $slowThresholdMs = 200.0,
        private readonly bool $logReads = false,
        int $priority = self::PRIORITY,
    ) {
        parent::__construct($priority);
    }

    #[Override]
    public function handle(Operation $operation, callable $next): mixed
    {
        // 读操作（size/stats）频次极高，默认不记录，避免日志淹没
        if (!$this->logReads && $operation->isRead() && $operation->name !== Operation::POP) {
            return $next($operation);
        }

        $startedAt = hrtime(true);

        try {
            $result = $next($operation);
            $elapsedMs = (hrtime(true) - $startedAt) / 1e6;

            $this->logger->log(
                $elapsedMs >= $this->slowThresholdMs ? LogLevel::WARNING : LogLevel::DEBUG,
                $elapsedMs >= $this->slowThresholdMs
                    ? '队列操作耗时超过阈值'
                    : '队列操作完成',
                $this->context($operation, $elapsedMs),
            );

            return $result;
        } catch (Throwable $e) {
            $this->logger->error('队列操作失败', [
                ...$this->context($operation, (hrtime(true) - $startedAt) / 1e6),
                'exception' => $e::class,
                'message' => $e->getMessage(),
            ]);

            throw $e;
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function context(Operation $operation, float $elapsedMs): array
    {
        return array_filter([
            'operation' => $operation->name,
            'connection' => $operation->connection,
            'queue' => $operation->queue,
            'job_id' => $operation->job->id ?? $operation->reserved?->job->id,
            'job_name' => $operation->job->name ?? $operation->reserved?->job->name,
            'trace_id' => $operation->job?->traceId(),
            'duration_ms' => round($elapsedMs, 3),
        ], static fn (mixed $v): bool => $v !== null);
    }
}
