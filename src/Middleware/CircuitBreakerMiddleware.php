<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Kode\Queue\Exception\QueueException;
use Override;
use Throwable;

/**
 * 熔断中间件。
 *
 * 队列后端整体故障时，继续无脑重试只会把连接池、Worker 全部拖死。
 * 熔断器在连续失败达到阈值后直接快速失败，冷却期结束再放行一个探测请求。
 *
 * 状态机：Closed（正常）→ Open（熔断）→ HalfOpen（半开探测）→ Closed / Open
 */
final class CircuitBreakerMiddleware extends AbstractMiddleware
{
    public const int PRIORITY = 850;

    private const string STATE_CLOSED = 'closed';
    private const string STATE_OPEN = 'open';
    private const string STATE_HALF_OPEN = 'half_open';

    /** @var array<string, array{state: string, failures: int, successes: int, openedAt: float}> */
    private array $circuits = [];

    /**
     * @param int   $failureThreshold 连续失败多少次后熔断
     * @param float $cooldownSeconds  熔断持续时长
     * @param int   $successThreshold 半开状态下连续成功多少次才恢复
     */
    public function __construct(
        private readonly int $failureThreshold = 5,
        private readonly float $cooldownSeconds = 30.0,
        private readonly int $successThreshold = 2,
        int $priority = self::PRIORITY,
    ) {
        parent::__construct($priority);
    }

    #[Override]
    public function handle(Operation $operation, callable $next): mixed
    {
        $key = $operation->connection . ':' . $operation->queue;
        $circuit = $this->circuit($key);

        if ($circuit['state'] === self::STATE_OPEN) {
            if (microtime(true) - $circuit['openedAt'] < $this->cooldownSeconds) {
                throw new QueueException(
                    sprintf('队列 [%s] 熔断中，%.1fs 后重试', $key, $this->remaining($circuit)),
                    context: ['queue' => $operation->queue, 'state' => self::STATE_OPEN],
                );
            }

            $this->circuits[$key]['state'] = self::STATE_HALF_OPEN;
            $this->circuits[$key]['successes'] = 0;
        }

        try {
            $result = $next($operation);
            $this->recordSuccess($key);

            return $result;
        } catch (Throwable $e) {
            $this->recordFailure($key);

            throw $e;
        }
    }

    /**
     * 当前状态快照（供监控暴露）。
     *
     * @return array<string, array{state: string, failures: int, successes: int, openedAt: float}>
     */
    public function snapshot(): array
    {
        return $this->circuits;
    }

    /**
     * @return array{state: string, failures: int, successes: int, openedAt: float}
     */
    private function circuit(string $key): array
    {
        return $this->circuits[$key] ??= [
            'state' => self::STATE_CLOSED,
            'failures' => 0,
            'successes' => 0,
            'openedAt' => 0.0,
        ];
    }

    private function recordSuccess(string $key): void
    {
        $circuit = $this->circuit($key);

        if ($circuit['state'] === self::STATE_HALF_OPEN) {
            $this->circuits[$key]['successes']++;
            if ($this->circuits[$key]['successes'] >= $this->successThreshold) {
                $this->circuits[$key] = [
                    'state' => self::STATE_CLOSED,
                    'failures' => 0,
                    'successes' => 0,
                    'openedAt' => 0.0,
                ];
            }

            return;
        }

        $this->circuits[$key]['failures'] = 0;
    }

    private function recordFailure(string $key): void
    {
        $this->circuit($key);
        $this->circuits[$key]['failures']++;

        if ($this->circuits[$key]['failures'] >= $this->failureThreshold) {
            $this->circuits[$key]['state'] = self::STATE_OPEN;
            $this->circuits[$key]['openedAt'] = microtime(true);
        }
    }

    /**
     * @param array{state: string, failures: int, successes: int, openedAt: float} $circuit
     */
    private function remaining(array $circuit): float
    {
        return max(0.0, $this->cooldownSeconds - (microtime(true) - $circuit['openedAt']));
    }
}
