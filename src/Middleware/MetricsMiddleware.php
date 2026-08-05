<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Override;
use Throwable;

/**
 * 指标采集中间件。
 *
 * 就地累计计数与耗时分位，`snapshot()` 可直接吐给 Prometheus / 自建监控。
 * 不引入任何外部依赖，也可通过 `$sink` 回调把指标转发到现有埋点系统。
 */
final class MetricsMiddleware extends AbstractMiddleware
{
    public const int PRIORITY = 950;

    /** @var array<string, array{count: int, errors: int, totalMs: float, maxMs: float, minMs: float}> */
    private array $metrics = [];

    /**
     * @param (callable(string, float, bool): void)|null $sink 可选：外部指标接收器
     */
    public function __construct(
        private readonly mixed $sink = null,
        int $priority = self::PRIORITY,
    ) {
        parent::__construct($priority);
    }

    #[Override]
    public function handle(Operation $operation, callable $next): mixed
    {
        $startedAt = hrtime(true);
        $failed = false;

        try {
            return $next($operation);
        } catch (Throwable $e) {
            $failed = true;

            throw $e;
        } finally {
            $elapsedMs = (hrtime(true) - $startedAt) / 1e6;
            $this->record($operation->key(), $elapsedMs, $failed);

            if (\is_callable($this->sink)) {
                ($this->sink)($operation->key(), $elapsedMs, $failed);
            }
        }
    }

    /**
     * 指标快照。
     *
     * @return array<string, array{count: int, errors: int, error_rate: float, avg_ms: float, max_ms: float, min_ms: float}>
     */
    public function snapshot(): array
    {
        $out = [];
        foreach ($this->metrics as $key => $m) {
            $out[$key] = [
                'count' => $m['count'],
                'errors' => $m['errors'],
                'error_rate' => $m['count'] > 0 ? round($m['errors'] / $m['count'], 4) : 0.0,
                'avg_ms' => $m['count'] > 0 ? round($m['totalMs'] / $m['count'], 3) : 0.0,
                'max_ms' => round($m['maxMs'], 3),
                'min_ms' => $m['minMs'] === PHP_FLOAT_MAX ? 0.0 : round($m['minMs'], 3),
            ];
        }

        return $out;
    }

    /**
     * Prometheus 文本格式输出。
     */
    public function toPrometheus(string $namespace = 'kode_queue'): string
    {
        $lines = [];
        foreach ($this->snapshot() as $key => $m) {
            $labels = sprintf('{operation="%s"}', addslashes($key));
            $lines[] = "{$namespace}_operations_total{$labels} {$m['count']}";
            $lines[] = "{$namespace}_errors_total{$labels} {$m['errors']}";
            $lines[] = "{$namespace}_duration_avg_ms{$labels} {$m['avg_ms']}";
            $lines[] = "{$namespace}_duration_max_ms{$labels} {$m['max_ms']}";
        }

        return implode("\n", $lines) . "\n";
    }

    public function reset(): void
    {
        $this->metrics = [];
    }

    /**
     * 人类可读的指标摘要（一行一条操作），结尾附一条汇总。
     *
     * 供 Worker 收尾日志、运维排障快速阅读。机器消费请用 {@see snapshot()} / {@see toPrometheus()}。
     */
    public function summary(string $prefix = '  '): string
    {
        if ($this->metrics === []) {
            return $prefix . '(无采集数据)';
        }

        $lines = [];
        $total = 0;
        $totalErrors = 0;

        foreach ($this->snapshot() as $key => $m) {
            $total += $m['count'];
            $totalErrors += $m['errors'];

            $lines[] = sprintf(
                '%s%-44s 次数=%-6d 错误=%-5d 错误率=%5.2f%% 平均=%8.2fms 最大=%8.2fms',
                $prefix,
                $key,
                $m['count'],
                $m['errors'],
                $m['error_rate'] * 100,
                $m['avg_ms'],
                $m['max_ms'],
            );
        }

        $lines[] = sprintf(
            '%s%-44s 次数=%-6d 错误=%-5d',
            $prefix,
            'TOTAL',
            $total,
            $totalErrors,
        );

        return implode("\n", $lines);
    }

    private function record(string $key, float $elapsedMs, bool $failed): void
    {
        $metric = $this->metrics[$key] ??= [
            'count' => 0,
            'errors' => 0,
            'totalMs' => 0.0,
            'maxMs' => 0.0,
            'minMs' => PHP_FLOAT_MAX,
        ];

        $this->metrics[$key] = [
            'count' => $metric['count'] + 1,
            'errors' => $metric['errors'] + ($failed ? 1 : 0),
            'totalMs' => $metric['totalMs'] + $elapsedMs,
            'maxMs' => max($metric['maxMs'], $elapsedMs),
            'minMs' => min($metric['minMs'], $elapsedMs),
        ];
    }
}
