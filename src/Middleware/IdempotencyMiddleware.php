<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Override;
use Psr\SimpleCache\CacheInterface;

/**
 * 幂等投递中间件（对接 **kode/cache** 或任何 PSR-16 实现）。
 *
 * 典型场景：下单接口重试导致同一笔订单被投递多次。
 * 本中间件按业务键去重，在 TTL 窗口内重复投递直接返回首次的任务 ID，
 * 不会真的进队列。
 *
 * 业务键取值优先级：
 * 1. `Job::$headers['idempotency_key']`
 * 2. 任务名 + 负载内容的 xxh128 摘要（PHP 8.1+ 内置，比 md5 快一个数量级）
 */
final class IdempotencyMiddleware extends AbstractMiddleware
{
    public const int PRIORITY = 960;

    /** 缓存键前缀 */
    public const string PREFIX = 'kode:queue:idem:';

    public function __construct(
        private readonly CacheInterface $cache,
        private readonly int $ttl = 300,
        int $priority = self::PRIORITY,
    ) {
        parent::__construct($priority);
    }

    #[Override]
    public function handle(Operation $operation, callable $next): mixed
    {
        if ($operation->name !== Operation::PUSH || $operation->job === null) {
            return $next($operation);
        }

        $key = self::PREFIX . $this->fingerprint($operation);

        /** @var string|null $existing */
        $existing = $this->cache->get($key);
        if (\is_string($existing) && $existing !== '') {
            // 命中：跳过真实投递，直接返回首次的任务 ID
            return $existing;
        }

        $jobId = $next($operation);

        if (\is_string($jobId) && $jobId !== '') {
            $this->cache->set($key, $jobId, $this->ttl);
        }

        return $jobId;
    }

    private function fingerprint(Operation $operation): string
    {
        $job = $operation->job;
        \assert($job !== null);

        $explicit = $job->headers['idempotency_key'] ?? null;
        if (\is_string($explicit) && $explicit !== '') {
            return $operation->queue . ':' . $explicit;
        }

        $payload = json_encode($job->payload, JSON_UNESCAPED_UNICODE | JSON_PARTIAL_OUTPUT_ON_ERROR) ?: '';

        // xxh128：非加密但极快的哈希，PHP 8.1 起内置，适合做去重指纹
        return $operation->queue . ':' . hash('xxh128', $job->name . '|' . $payload);
    }
}
