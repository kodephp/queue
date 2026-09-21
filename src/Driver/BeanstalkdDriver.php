<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Enum\DriverType;
use Kode\Queue\Enum\JobStatus;
use Kode\Queue\Exception\DriverException;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;
use Override;

/**
 * Beanstalkd 驱动。
 *
 * Beanstalkd 本身就把「预留 / 确认 / 掩埋」做进了协议，是最贴合队列语义的后端：
 * `reserve` 拿到的任务在 TTR 内对其他 Worker 不可见，超时自动回到 ready 状态。
 * 所以这里不需要像 Redis / 数据库那样自己实现可见性超时，直接把 TTR 交给它即可。
 *
 * v1 只用了 `put` / `reserve` 两个命令，`delete()` 甚至没有真正删除，
 * 导致任务在 reserved 状态堆积直到 TTR 到期反复重投。v2 完整接上四态流转，
 * 并把优先级语义翻译正确——Beanstalkd 是「数值越小越优先」，与内部约定相反。
 *
 * 同时兼容 Pheanstalk 4.x（字符串 tube）与 5.x（值对象 TubeName），
 * 用户不必为了升级队列库去动 Pheanstalk 版本。
 */
class BeanstalkdDriver extends AbstractDriver
{
    private ?object $client = null;

    /** Pheanstalk 5 起 tube 名要包成值对象 */
    private ?bool $usesValueObjects = null;

    private ?string $watching = null;

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Beanstalkd;
    }

    // ------------------------------------------------------------------
    // 生产
    // ------------------------------------------------------------------

    #[Override]
    public function push(Job $job): string
    {
        $job = $this->normalize($job);

        $this->guard('push', function () use ($job): void {
            $client = $this->client();
            $client->useTube($this->tube($job->queue));

            $client->put(
                $this->encode($job),
                $job->priority->toBeanstalkd(),
                $job->delaySeconds(),
                // TTR 即可见性超时：任务声明的 timeout 优先，退回连接级 retry_after
                $job->timeout > 0 ? $job->timeout : $this->retryAfter(),
            );
        }, ['job' => $job->id, 'queue' => $job->queue]);

        return $job->id;
    }

    // ------------------------------------------------------------------
    // 消费
    // ------------------------------------------------------------------

    #[Override]
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob
    {
        return $this->guard('pop', function () use ($queue, $timeout): ?ReservedJob {
            $client = $this->client();
            $this->watchOnly($queue);

            // Beanstalkd 原生支持阻塞 reserve，不需要 PHP 侧轮询
            $handle = $client->reserveWithTimeout(max(0, (int) ceil($timeout)));

            if ($handle === null) {
                return null;
            }

            $payload = (string) $handle->getData();
            $job = $this->decode($payload)->incrementAttempts();

            return ReservedJob::of(
                job: $job,
                receipt: $this->handleId($handle),
                rawPayload: $payload,
                meta: ['handle' => $handle, 'tube' => $queue],
            );
        }, ['queue' => $queue]);
    }

    #[Override]
    public function peek(string $queue): ?Job
    {
        return $this->guard('peek', function () use ($queue): ?Job {
            $client = $this->client();
            $client->useTube($this->tube($queue));

            try {
                $handle = $client->peekReady();
            } catch (\Throwable) {
                // 空 tube 在部分版本里以异常形式返回
                return null;
            }

            return $handle === null ? null : $this->decode((string) $handle->getData());
        }, ['queue' => $queue]);
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        return $this->guard('ack', function () use ($job): bool {
            $this->client()->delete($this->handleOf($job));

            return true;
        }, ['job' => $job->job->id]);
    }

    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        return $this->guard('release', function () use ($job, $delay): bool {
            $client = $this->client();
            $delay = max(0, $delay);

            // Beanstalkd 的 release 回投的是原始消息体，attempts 不会持久化，
            // 反复失败的任务每次 pop 都从旧负载重新 +1，maxAttempts 形同虚设。
            // 改为「先投新负载（含自增后的 attempts），再删旧 handle」：
            // 顺序保证最坏是重复投递而非丢任务。
            $next = $job->job->delayedBy($delay)->withStatus(
                $delay > 0 ? JobStatus::Delayed : JobStatus::Pending,
            );

            $client->useTube($this->tube($next->queue));
            $client->put(
                $this->encode($next),
                $next->priority->toBeanstalkd(),
                $delay,
                $next->timeout > 0 ? $next->timeout : $this->retryAfter(),
            );
            $client->delete($this->handleOf($job));

            return true;
        }, ['job' => $job->job->id, 'delay' => $delay]);
    }

    /**
     * 失败即掩埋（bury）：任务留在 buried 状态，可用 `kick` 人工重放，
     * 比直接删除更适合排障——这正是 Beanstalkd 相对其他后端的独特价值。
     */
    #[Override]
    public function fail(ReservedJob $job, \Throwable $reason): bool
    {
        return $this->guard('fail', function () use ($job): bool {
            $this->client()->bury($this->handleOf($job), $job->job->priority->toBeanstalkd());

            return true;
        }, ['job' => $job->job->id]);
    }

    /**
     * 把掩埋的任务重新放回 ready 队列。
     *
     * @return int 实际被踢回的数量
     */
    public function kick(string $queue, int $max = 100): int
    {
        return $this->guard('kick', function () use ($queue, $max): int {
            $client = $this->client();
            $client->useTube($this->tube($queue));

            return (int) $client->kick($max);
        }, ['queue' => $queue]);
    }

    // ------------------------------------------------------------------
    // 管理
    // ------------------------------------------------------------------

    #[Override]
    public function size(string $queue): int
    {
        return $this->stats($queue)->ready;
    }

    #[Override]
    public function clear(string $queue): int
    {
        // Beanstalkd 没有 flush 命令，逐个 reserve + delete 会误伤其他 Worker，
        // 因此这里明确不做破坏性操作，返回 0 表示未清理。
        return 0;
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        return $this->guard('stats', function () use ($queue): QueueStats {
            try {
                $stats = $this->client()->statsTube($this->tube($queue));
            } catch (\Throwable) {
                // tube 尚不存在时 Beanstalkd 会报 NOT_FOUND
                return QueueStats::of(queue: $queue, driver: $this->type()->value);
            }

            $read = static fn (string $key): int => (int) ($stats[$key] ?? 0);

            return QueueStats::of(
                queue: $queue,
                driver: $this->type()->value,
                ready: $read('current-jobs-ready'),
                delayed: $read('current-jobs-delayed'),
                reserved: $read('current-jobs-reserved'),
                failed: $read('current-jobs-buried'),
                extra: [
                    'buried' => $read('current-jobs-buried'),
                    'total' => $read('total-jobs'),
                    'watching' => $read('current-watching'),
                ],
            );
        }, ['queue' => $queue]);
    }

    // ------------------------------------------------------------------
    // 连接
    // ------------------------------------------------------------------

    #[Override]
    public function isConnected(): bool
    {
        if ($this->client === null) {
            return false;
        }

        try {
            $this->client->stats();

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    #[Override]
    public function close(): void
    {
        if ($this->client !== null && method_exists($this->client, 'disconnect')) {
            try {
                $this->client->disconnect();
            } catch (\Throwable) {
                // 忽略关闭异常
            }
        }

        $this->client = null;
        $this->watching = null;
        $this->connected = false;
    }

    public function client(): object
    {
        if ($this->client !== null) {
            return $this->client;
        }

        $injected = $this->config->object('client_instance') ?? $this->config->object('connection');

        if ($injected !== null) {
            $this->connected = true;

            return $this->client = $injected;
        }

        /** @var class-string $class */
        $class = 'Pheanstalk\\Pheanstalk';

        if (!class_exists($class)) {
            throw DriverException::missingDependency(DriverType::Beanstalkd);
        }

        /** @var callable $factory */
        $factory = [$class, 'create'];
        $client = $factory(
            $this->config->string('host', '127.0.0.1'),
            $this->config->int('port', 11300),
        );

        $this->connected = true;

        return $this->client = $client;
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /**
     * 只监听目标 tube，避免 reserve 到别的队列的任务。
     */
    private function watchOnly(string $queue): void
    {
        if ($this->watching === $queue) {
            return;
        }

        $client = $this->client();
        $client->watch($this->tube($queue));

        if ($this->watching !== null && $this->watching !== $queue) {
            $client->ignore($this->tube($this->watching));
        } elseif ($queue !== 'default') {
            try {
                $client->ignore($this->tube('default'));
            } catch (\Throwable) {
                // 不允许 ignore 最后一个 tube，忽略
            }
        }

        $this->watching = $queue;
    }

    /**
     * Pheanstalk 4 收字符串，5 收 TubeName 值对象。
     */
    private function tube(string $name): string|object
    {
        $this->usesValueObjects ??= class_exists('Pheanstalk\\Values\\TubeName');

        if (!$this->usesValueObjects) {
            return $name;
        }

        /** @var class-string $class */
        $class = 'Pheanstalk\\Values\\TubeName';

        return new $class($name);
    }

    private function handleOf(ReservedJob $job): object
    {
        $handle = $job->meta['handle'] ?? null;

        if (\is_object($handle)) {
            return $handle;
        }

        // 跨进程确认时只剩下数字 ID，用值对象重建
        foreach (['Pheanstalk\\Values\\JobId', 'Pheanstalk\\Job'] as $class) {
            if (class_exists($class)) {
                return new $class((int) $job->receipt);
            }
        }

        throw DriverException::operationFailed(
            'beanstalkd.handle',
            new \RuntimeException('无法还原 Beanstalkd 任务句柄'),
            ['job' => $job->job->id],
        );
    }

    private function handleId(object $handle): int|string
    {
        return match (true) {
            method_exists($handle, 'getId') => $handle->getId(),
            property_exists($handle, 'id') => $handle->id,
            default => (string) spl_object_id($handle),
        };
    }
}
