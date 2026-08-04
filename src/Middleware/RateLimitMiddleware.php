<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Kode\Queue\Exception\QueueException;
use Override;

/**
 * 限流中间件。
 *
 * 优先复用 **kode/limiting**：只要容器里给了 `Kode\Limiting\Limiter`，
 * 就能直接享受它的令牌桶 / 滑动窗口 / Redis 分布式限流；
 * 没装该包时退化为内置的进程内令牌桶，行为一致、无需改代码。
 *
 * v1 的实现有个隐蔽 bug：`usleep()` 之后无条件扣减令牌，
 * 高并发下会把令牌扣成负数，导致后续请求全部串行等待。这里已修正。
 */
final class RateLimitMiddleware extends AbstractMiddleware
{
    public const int PRIORITY = 700;

    /** kode/limiting 的 Limiter（若可用） */
    private readonly ?object $limiter;

    private float $tokens;

    private float $lastRefill;

    /**
     * @param int         $capacity 桶容量（突发上限）
     * @param float       $rate     每秒补充的令牌数
     * @param bool        $blocking true = 等待令牌；false = 立即抛异常
     * @param object|null $limiter  kode/limiting 的 Limiter 实例（可选）
     * @param string|null $key      限流键，null 时按「连接:队列:操作」自动分组
     */
    public function __construct(
        private readonly int $capacity = 100,
        private readonly float $rate = 50.0,
        private readonly bool $blocking = true,
        ?object $limiter = null,
        private readonly ?string $key = null,
        int $priority = self::PRIORITY,
    ) {
        parent::__construct($priority);

        $this->limiter = $limiter !== null && method_exists($limiter, 'allow') ? $limiter : null;
        $this->tokens = $capacity;
        $this->lastRefill = microtime(true);
    }

    /**
     * 用 kode/limiting 构建：`RateLimitMiddleware::withKodeLimiting(100, 50)`。
     *
     * 未安装该包时自动回退到内置实现，调用方无需做任何判断。
     */
    public static function withKodeLimiting(
        int $capacity = 100,
        float $rate = 50.0,
        bool $blocking = true,
        ?string $key = null,
    ): self {
        $limiterClass = 'Kode\\Limiting\\Limiter';

        if (!class_exists($limiterClass)) {
            return new self($capacity, $rate, $blocking, key: $key);
        }

        /** @var object $limiter */
        $limiter = $limiterClass::tokenBucket($capacity, $rate);

        return new self($capacity, $rate, $blocking, limiter: $limiter, key: $key);
    }

    #[Override]
    public function handle(Operation $operation, callable $next): mixed
    {
        $key = $this->key ?? $operation->key();

        if (!$this->acquire($key)) {
            throw new QueueException(
                sprintf('队列操作 [%s] 触发限流（%.1f 次/秒）', $operation->name, $this->rate),
                context: ['operation' => $operation->name, 'queue' => $operation->queue, 'key' => $key],
            );
        }

        return $next($operation);
    }

    /**
     * 获取一个令牌。
     */
    private function acquire(string $key): bool
    {
        if ($this->limiter !== null) {
            return $this->acquireFromLimiter($key);
        }

        $this->refill();

        if ($this->tokens >= 1.0) {
            $this->tokens -= 1.0;

            return true;
        }

        if (!$this->blocking) {
            return false;
        }

        // 精确计算「再等多久刚好有一个令牌」，等待后原子扣减
        $waitSeconds = (1.0 - $this->tokens) / max($this->rate, 0.0001);
        usleep((int) ($waitSeconds * 1_000_000));
        $this->refill();
        $this->tokens = max(0.0, $this->tokens - 1.0);

        return true;
    }

    private function acquireFromLimiter(string $key): bool
    {
        /** @var object&object{allow: callable, getWaitTime?: callable} $limiter */
        $limiter = $this->limiter;

        if ($limiter->allow($key)) {
            return true;
        }

        if (!$this->blocking) {
            return false;
        }

        if (method_exists($limiter, 'getWaitTime')) {
            $wait = (float) $limiter->getWaitTime($key);
            if ($wait > 0) {
                usleep((int) ($wait * 1_000_000));
            }
        }

        return $limiter->allow($key);
    }

    private function refill(): void
    {
        $now = microtime(true);
        $this->tokens = min($this->capacity, $this->tokens + ($now - $this->lastRefill) * $this->rate);
        $this->lastRefill = $now;
    }
}
