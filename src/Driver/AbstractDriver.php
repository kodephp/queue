<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Closure;
use Kode\Queue\Config\ConnectionConfig;
use Kode\Queue\Contract\DriverInterface;
use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Enum\Capability;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Exception\DriverException;
use Kode\Queue\Exception\TransactionException;
use Kode\Queue\Exception\UnsupportedFeatureException;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Serializer\SerializerFactory;
use Kode\Queue\Support\QueueStats;
use Override;

/**
 * 驱动抽象基类。
 *
 * 把所有驱动都要写一遍的样板收敛到这里：
 * 依赖自检、惰性连接、序列化、能力校验、闭包事务、批量投递降级。
 * 子类只需实现真正与后端相关的少数几个 `doXxx()` 方法。
 */
abstract class AbstractDriver implements DriverInterface
{
    protected readonly SerializerInterface $serializer;

    /** @var list<Capability> */
    protected readonly array $capabilities;

    /** 事务嵌套深度 */
    protected int $transactionLevel = 0;

    protected bool $connected = false;

    public function __construct(
        protected readonly ConnectionConfig $config,
        ?SerializerInterface $serializer = null,
    ) {
        $type = $this->type();

        if (!$type->isAvailable()) {
            throw DriverException::missingDependency($type);
        }

        $this->serializer = $serializer ?? SerializerFactory::make($config->serializer);
        $this->capabilities = $type->capabilities();
    }

    // ------------------------------------------------------------------
    // 元信息
    // ------------------------------------------------------------------

    #[Override]
    public function supports(Capability $capability): bool
    {
        return \in_array($capability, $this->capabilities, true);
    }

    /**
     * 校验能力，不支持直接抛异常（用在必须依赖该能力的路径上）。
     */
    protected function requireCapability(Capability $capability): void
    {
        if (!$this->supports($capability)) {
            throw UnsupportedFeatureException::for($this->type()->value, $capability);
        }
    }

    public function config(): ConnectionConfig
    {
        return $this->config;
    }

    public function serializer(): SerializerInterface
    {
        return $this->serializer;
    }

    // ------------------------------------------------------------------
    // 默认实现
    // ------------------------------------------------------------------

    /**
     * 批量投递默认逐条落库；具备原生批量能力的驱动应覆写本方法。
     */
    #[Override]
    public function pushBatch(iterable $jobs): array
    {
        $ids = [];
        foreach ($jobs as $job) {
            $ids[] = $this->push($job);
        }

        return $ids;
    }

    #[Override]
    public function peek(string $queue): ?Job
    {
        return null;
    }

    #[Override]
    public function clear(string $queue): int
    {
        return 0;
    }

    #[Override]
    public function reclaimExpired(string $queue): int
    {
        return 0;
    }

    #[Override]
    public function fail(ReservedJob $job, \Throwable $reason): bool
    {
        // 默认语义：不支持死信的后端直接确认，由上层 FailedJobStore 负责留档
        return $this->ack($job);
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        return QueueStats::of(
            queue: $queue,
            driver: $this->type()->value,
            ready: $this->size($queue),
        );
    }

    #[Override]
    public function transaction(Closure $callback): mixed
    {
        if (!$this->supports(Capability::Transaction)) {
            // 不支持事务的后端：直接执行，语义上等价于「自动提交」
            return $callback($this);
        }

        if ($this->transactionLevel > 0) {
            throw TransactionException::alreadyActive();
        }

        $this->beginTransaction();
        $this->transactionLevel++;

        try {
            $result = $callback($this);
            $this->commitTransaction();

            return $result;
        } catch (\Throwable $e) {
            try {
                $this->rollbackTransaction();
            } catch (\Throwable $rollbackError) {
                throw TransactionException::rollbackFailed($rollbackError);
            }

            throw $e;
        } finally {
            $this->transactionLevel = 0;
        }
    }

    #[Override]
    public function isConnected(): bool
    {
        return $this->connected;
    }

    #[Override]
    public function reconnect(): void
    {
        $this->close();
        $this->connected = false;
    }

    // ------------------------------------------------------------------
    // 供子类覆写的事务钩子
    // ------------------------------------------------------------------

    protected function beginTransaction(): void
    {
    }

    protected function commitTransaction(): void
    {
    }

    protected function rollbackTransaction(): void
    {
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    /**
     * 队列 key 前缀（Redis / Kafka 等需要命名空间的后端使用）。
     */
    protected function prefix(): string
    {
        return $this->config->string('prefix', '') ?? '';
    }

    /**
     * 可见性超时：预留超过该时长的任务会被 `reclaimExpired()` 回收。
     */
    protected function retryAfter(): int
    {
        return max(1, $this->config->retryAfter);
    }

    protected function encode(Job $job): string
    {
        return $this->serializer->serialize($job);
    }

    protected function decode(string $raw): Job
    {
        return $this->serializer->deserialize($raw);
    }

    /**
     * 统一包装后端异常，附带操作上下文。
     *
     * @template T
     * @param  Closure(): T        $operation
     * @param  array<string, mixed> $context
     * @return T
     */
    protected function guard(string $name, Closure $operation, array $context = []): mixed
    {
        try {
            return $operation();
        } catch (DriverException | UnsupportedFeatureException $e) {
            throw $e;
        } catch (\Throwable $e) {
            throw DriverException::operationFailed($name, $e, [
                'driver' => $this->type()->value,
                ...$context,
            ]);
        }
    }

    /**
     * 把任务标准化到目标队列（保证 `Job::$queue` 与实际投递队列一致）。
     */
    protected function normalize(Job $job, ?string $queue = null): Job
    {
        $target = $queue ?? $job->queue ?? $this->config->queue;

        return $job->queue === $target ? $job : $job->withQueue($target);
    }

    abstract public function type(): DriverType;
}
