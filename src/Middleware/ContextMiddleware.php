<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Kode\Queue\Integration\KodeContextBridge;
use Kode\Queue\Support\IdGenerator;
use Override;

/**
 * 上下文透传中间件（对接 **kode/context**）。
 *
 * 队列是天然的「链路断点」：HTTP 请求里的 trace_id、租户 ID、用户身份
 * 一旦任务入队就丢了，消费端日志与生产端日志再也对不上。
 *
 * 本中间件在 push 时把上下文快照写进 `Job::$headers`，
 * 在 pop 时自动 restore 回当前执行上下文——协程 / 多进程 / 多线程下都成立
 * （kode/context 内部已按运行时隔离存储）。
 */
final class ContextMiddleware extends AbstractMiddleware
{
    public const int PRIORITY = 990;

    /** 默认透传的上下文键 */
    public const array DEFAULT_KEYS = ['trace_id', 'span_id', 'tenant_id', 'user_id', 'locale'];

    /**
     * @param list<string> $keys        需要透传的上下文键
     * @param bool         $autoTraceId 上游没有 trace_id 时自动生成
     */
    public function __construct(
        private readonly array $keys = self::DEFAULT_KEYS,
        private readonly bool $autoTraceId = true,
        int $priority = self::PRIORITY,
    ) {
        parent::__construct($priority);
    }

    #[Override]
    public function handle(Operation $operation, callable $next): mixed
    {
        // ---- 生产侧：把当前上下文注入任务头 ----
        if ($operation->job !== null) {
            $headers = KodeContextBridge::export($this->keys);

            if ($this->autoTraceId && !isset($headers['trace_id'])) {
                $headers['trace_id'] = IdGenerator::ulid();
            }

            $headers['queued_at'] = microtime(true);
            $operation = $operation->withJob($operation->job->withHeaders($headers));
        }

        $result = $next($operation);

        // ---- 消费侧：把任务头还原到当前上下文 ----
        if ($operation->name === Operation::POP && \is_object($result)) {
            /** @var object{job?: object} $result */
            $job = $result->job ?? null;
            if (\is_object($job) && property_exists($job, 'headers')) {
                /** @var array<string, mixed> $headers */
                $headers = $job->headers;
                KodeContextBridge::import($headers, $this->keys);
            }
        }

        return $result;
    }
}
