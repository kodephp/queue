<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Exception\QueueException;
use Override;
use Throwable;

/**
 * 操作级重试中间件（针对后端抖动，不是任务业务重试）。
 *
 * 对 v1 的三点修正：
 * 1. **只捕获 `\Exception`** → 现在捕获 `\Throwable`，`TypeError` 之类也能触发重试或正确冒泡；
 * 2. **固定倍率** → 改用 {@see BackoffStrategy}，默认带抖动，避免多进程同时重试打垮后端；
 * 3. **无差别重试** → 支持白名单 / 黑名单，业务异常（如参数非法）不该被重试 N 次。
 */
final class RetryMiddleware extends AbstractMiddleware
{
    public const int PRIORITY = 800;

    /**
     * @param int                       $maxAttempts 最大尝试次数（含首次）
     * @param BackoffStrategy           $strategy    退避策略
     * @param int                       $baseDelay   基础间隔（毫秒）
     * @param list<class-string>        $retryOn     仅重试这些异常，空表示全部
     * @param list<class-string>        $noRetryOn   永不重试这些异常
     * @param (callable(Throwable):bool)|null $decider 自定义判定（优先级最高）
     */
    public function __construct(
        private readonly int $maxAttempts = 3,
        private readonly BackoffStrategy $strategy = BackoffStrategy::ExponentialJitter,
        private readonly int $baseDelay = 100,
        private readonly array $retryOn = [],
        private readonly array $noRetryOn = [],
        private readonly mixed $decider = null,
        int $priority = self::PRIORITY,
    ) {
        parent::__construct($priority);
    }

    #[Override]
    public function handle(Operation $operation, callable $next): mixed
    {
        $attempt = 0;
        $lastError = null;

        while ($attempt < $this->maxAttempts) {
            $attempt++;

            try {
                return $next($operation->withContext(['retry_attempt' => $attempt]));
            } catch (Throwable $e) {
                $lastError = $e;

                if ($attempt >= $this->maxAttempts || !$this->shouldRetry($e)) {
                    break;
                }

                // 毫秒级退避：delayFor 以秒为单位，这里按毫秒基数换算
                $delayMs = $this->strategy->delayFor($attempt, max(1, $this->baseDelay), 0);
                usleep($delayMs * 1000);
            }
        }

        throw new QueueException(
            sprintf(
                '队列操作 [%s] 重试 %d 次后仍然失败：%s',
                $operation->name,
                $attempt,
                $lastError?->getMessage() ?? '未知错误',
            ),
            previous: $lastError,
            context: [
                'operation' => $operation->name,
                'queue' => $operation->queue,
                'attempts' => $attempt,
                'strategy' => $this->strategy->value,
            ],
        );
    }

    private function shouldRetry(Throwable $e): bool
    {
        if (\is_callable($this->decider)) {
            return (bool) ($this->decider)($e);
        }

        if (array_any($this->noRetryOn, fn($class) => $e instanceof $class)) {
            return false;
        }

        if ($this->retryOn === []) {
            return true;
        }

        return array_any($this->retryOn, fn($class) => $e instanceof $class);
    }
}
