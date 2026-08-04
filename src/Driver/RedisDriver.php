<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Driver\Redis\LuaScripts;
use Kode\Queue\Driver\Redis\RedisClientFactory;
use Kode\Queue\Driver\Redis\RedisClientInterface;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Enum\JobStatus;
use Kode\Queue\Integration\KodeRuntimeBridge;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;
use Override;

/**
 * Redis 驱动。
 *
 * 相对 v1 的三处实质性改造：
 *
 * 1. **不再丢任务**。v1 用 `LPOP` 取走消息就算完事，Worker 崩溃 = 消息蒸发。
 *    v2 引入 `reserved` 有序集合与可见性超时：取走的消息只是「暂时不可见」，
 *    超过 `retry_after` 未确认会被自动放回，实现真正的至少一次投递。
 * 2. **不再靠多条命令拼凑**。搬运延迟任务、回收超时任务、弹出、预留
 *    全部在同一段 Lua 里完成，多 Worker 并发下不会重复消费。
 * 3. **支持优先级**。就绪队列改用 ZSET，分值 = 到达时间 - 优先级偏移，
 *    高优先级任务相当于「提前到达」，同时因为分值仍随时间推移，
 *    低优先级任务不会被无限饿死。
 *
 * 事务语义采用「缓冲投递」：事务期间 `push()` 只入内存缓冲，
 * 提交时用一条 Lua 原子写入。这比 Redis 原生 MULTI 更贴合队列场景——
 * 业务真正需要的是「要么全部入队，要么一条都不入队」。
 */
class RedisDriver extends AbstractDriver
{
    private ?RedisClientInterface $client = null;

    /** @var list<Job>|null 事务缓冲区，null 表示当前不在事务中 */
    private ?array $buffer = null;

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Redis;
    }

    // ------------------------------------------------------------------
    // 生产
    // ------------------------------------------------------------------

    #[Override]
    public function push(Job $job): string
    {
        $job = $this->normalize($job);

        if ($this->buffer !== null) {
            $this->buffer[] = $job;

            return $job->id;
        }

        $this->pushBatch([$job]);

        return $job->id;
    }

    #[Override]
    public function pushBatch(iterable $jobs): array
    {
        $ids = [];
        $argv = [];
        $queue = null;

        foreach ($jobs as $job) {
            $job = $this->normalize($job);

            // 一次脚本只能操作同一组 KEYS，跨队列时按队列切分批次
            if ($queue !== null && $job->queue !== $queue) {
                $this->flush($queue, $argv);
                $argv = [];
            }

            $queue = $job->queue;
            $delayed = !$job->isReady();

            $argv[] = $this->encode($job);
            $argv[] = $delayed
                ? (string) $job->availableAt
                : (string) $job->priority->toRedisScore(microtime(true));
            $argv[] = $delayed ? '1' : '0';

            $ids[] = $job->id;
        }

        if ($queue !== null && $argv !== []) {
            $this->flush($queue, $argv);
        }

        return $ids;
    }

    // ------------------------------------------------------------------
    // 消费
    // ------------------------------------------------------------------

    #[Override]
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob
    {
        $deadline = microtime(true) + max(0.0, $timeout);
        $interval = max(0.001, $this->config->float('poll_interval', 0.05));

        do {
            $reserved = $this->reserveOne($queue);

            if ($reserved !== null) {
                return $reserved;
            }

            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                break;
            }

            // 协程环境下让出调度权，不阻塞同进程的其他协程
            KodeRuntimeBridge::sleep(min($interval, $remaining));
        } while (microtime(true) < $deadline);

        return null;
    }

    #[Override]
    public function peek(string $queue): ?Job
    {
        return $this->guard('peek', function () use ($queue): ?Job {
            $payload = $this->client()->firstMember($this->key($queue));

            return $payload === null ? null : $this->decode($payload);
        }, ['queue' => $queue]);
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        return $this->guard('ack', function () use ($job): bool {
            $removed = $this->client()->evaluate(
                LuaScripts::ACK,
                [$this->key($job->job->queue, 'reserved')],
                [$job->rawPayload !== '' ? $job->rawPayload : (string) $job->receipt],
            );

            return (int) $removed > 0;
        }, ['job' => $job->job->id]);
    }

    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        return $this->guard('release', function () use ($job, $delay): bool {
            $delay = max(0, $delay);
            $next = $job->job->delayedBy($delay)->withStatus(
                $delay > 0 ? JobStatus::Delayed : JobStatus::Pending,
            );

            $removed = $this->client()->evaluate(
                LuaScripts::RELEASE,
                [
                    $this->key($next->queue),
                    $this->key($next->queue, 'delayed'),
                    $this->key($job->job->queue, 'reserved'),
                ],
                [
                    $job->rawPayload !== '' ? $job->rawPayload : (string) $job->receipt,
                    $this->encode($next),
                    $delay > 0
                        ? (string) $next->availableAt
                        : (string) $next->priority->toRedisScore(microtime(true)),
                    $delay > 0 ? '1' : '0',
                ],
            );

            return (int) $removed >= 0;
        }, ['job' => $job->job->id, 'delay' => $delay]);
    }

    #[Override]
    public function fail(ReservedJob $job, \Throwable $reason): bool
    {
        return $this->guard('fail', function () use ($job, $reason): bool {
            $keep = $this->config->int('dead_letter_size', 1000);
            $record = $this->config->bool('dead_letter', true)
                ? json_encode([
                    ...$job->job->toArray(),
                    'failed_at' => time(),
                    'exception' => $reason::class,
                    'message' => $reason->getMessage(),
                ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR)
                : '';

            $removed = $this->client()->evaluate(
                LuaScripts::FAIL,
                [
                    $this->key($job->job->queue, 'reserved'),
                    $this->key($job->job->queue, 'failed'),
                ],
                [
                    $job->rawPayload !== '' ? $job->rawPayload : (string) $job->receipt,
                    $record,
                    (string) $keep,
                ],
            );

            return (int) $removed >= 0;
        }, ['job' => $job->job->id]);
    }

    // ------------------------------------------------------------------
    // 管理
    // ------------------------------------------------------------------

    #[Override]
    public function size(string $queue): int
    {
        return $this->guard(
            'size',
            fn (): int => $this->client()->zcard($this->key($queue)),
            ['queue' => $queue],
        );
    }

    #[Override]
    public function clear(string $queue): int
    {
        return $this->guard('clear', function () use ($queue): int {
            return (int) $this->client()->evaluate(LuaScripts::CLEAR, [
                $this->key($queue),
                $this->key($queue, 'delayed'),
                $this->key($queue, 'reserved'),
            ]);
        }, ['queue' => $queue]);
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        return $this->guard('stats', function () use ($queue): QueueStats {
            /** @var list<int|string> $result */
            $result = (array) $this->client()->evaluate(LuaScripts::STATS, [
                $this->key($queue),
                $this->key($queue, 'delayed'),
                $this->key($queue, 'reserved'),
                $this->key($queue, 'failed'),
            ], ['0']);

            return QueueStats::of(
                queue: $queue,
                driver: $this->type()->value,
                ready: (int) ($result[0] ?? 0),
                delayed: (int) ($result[1] ?? 0),
                reserved: (int) ($result[2] ?? 0),
                failed: (int) ($result[3] ?? -1),
                extra: ['prefix' => $this->prefix()],
            );
        }, ['queue' => $queue]);
    }

    #[Override]
    public function reclaimExpired(string $queue): int
    {
        return $this->guard('reclaimExpired', function () use ($queue): int {
            return (int) $this->client()->evaluate(
                LuaScripts::reclaim(),
                [$this->key($queue), $this->key($queue, 'reserved')],
                [(string) time(), (string) LuaScripts::MIGRATE_LIMIT],
            );
        }, ['queue' => $queue]);
    }

    // ------------------------------------------------------------------
    // 事务：缓冲投递
    // ------------------------------------------------------------------

    #[Override]
    protected function beginTransaction(): void
    {
        $this->buffer = [];
    }

    #[Override]
    protected function commitTransaction(): void
    {
        $pending = $this->buffer ?? [];
        $this->buffer = null;

        if ($pending !== []) {
            $this->pushBatch($pending);
        }
    }

    #[Override]
    protected function rollbackTransaction(): void
    {
        $this->buffer = null;
    }

    // ------------------------------------------------------------------
    // 连接
    // ------------------------------------------------------------------

    #[Override]
    public function isConnected(): bool
    {
        return $this->client?->ping() ?? false;
    }

    #[Override]
    public function reconnect(): void
    {
        $this->close();
        $this->client();
    }

    #[Override]
    public function close(): void
    {
        $this->client?->disconnect();
        $this->client = null;
        $this->connected = false;
    }

    /**
     * 暴露底层客户端，便于复用同一连接执行分布式限流等扩展逻辑。
     */
    public function client(): RedisClientInterface
    {
        if ($this->client === null) {
            $this->client = RedisClientFactory::make($this->config);
            $this->connected = true;
        }

        return $this->client;
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private function reserveOne(string $queue): ?ReservedJob
    {
        return $this->guard('pop', function () use ($queue): ?ReservedJob {
            $now = time();

            $result = $this->client()->evaluate(
                LuaScripts::reserve(),
                [
                    $this->key($queue),
                    $this->key($queue, 'delayed'),
                    $this->key($queue, 'reserved'),
                ],
                [
                    (string) $now,
                    (string) ($now + $this->retryAfter()),
                    (string) LuaScripts::MIGRATE_LIMIT,
                ],
            );

            if (!\is_array($result) || !isset($result[0]) || !\is_string($result[0])) {
                return null;
            }

            [$payload, $rewritten] = [$result[0], (int) ($result[1] ?? 0)];
            $job = $this->decode($payload);

            // 非 JSON 序列化器下 Lua 无法改写负载，attempts 由 PHP 侧补上
            if ($rewritten === 0) {
                $job = $job->incrementAttempts();
            }

            return ReservedJob::of($job, $job->id, $payload, [
                'reserved_until' => $now + $this->retryAfter(),
            ]);
        }, ['queue' => $queue]);
    }

    /**
     * @param list<string> $argv
     */
    private function flush(string $queue, array $argv): void
    {
        if ($argv === []) {
            return;
        }

        $this->guard('push', function () use ($queue, $argv): void {
            $this->client()->evaluate(
                LuaScripts::PUSH,
                [$this->key($queue), $this->key($queue, 'delayed')],
                $argv,
            );
        }, ['queue' => $queue, 'count' => \intdiv(\count($argv), 3)]);
    }

    /**
     * 构造 Redis 键名。
     */
    private function key(string $queue, string $suffix = ''): string
    {
        $base = $this->prefix() . $queue;

        return $suffix === '' ? $base : $base . ':' . $suffix;
    }
}
