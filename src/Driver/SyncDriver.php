<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Config\ConnectionConfig;
use Kode\Queue\Contract\HandlerResolverInterface;
use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Message\Job;
use Override;

/**
 * 同步驱动：`push()` 即刻执行，不真正入队。
 *
 * 与 v1 最大的区别：v1 名为 Sync 实为「内存数组」，任务其实并不会执行；
 * v2 的 Sync 会立刻通过 {@see HandlerResolverInterface} 解析处理器并同步执行——
 * 这才是「同步驱动」应有的语义，本地调试时无需再起 Worker。
 *
 * 未注入解析器时，退化为 {@see MemoryDriver} 的行为（仅入内存队列）。
 */
final class SyncDriver extends MemoryDriver
{
    private ?HandlerResolverInterface $resolver;

    public function __construct(
        ConnectionConfig $config,
        ?SerializerInterface $serializer = null,
        ?HandlerResolverInterface $resolver = null,
    ) {
        parent::__construct($config, $serializer);

        $this->resolver = $resolver ?? ($config->object('resolver') instanceof HandlerResolverInterface
            ? $config->object('resolver')
            : null);
    }

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Sync;
    }

    /**
     * 注入处理器解析器（QueueManager 在构建时调用）。
     */
    public function setResolver(?HandlerResolverInterface $resolver): void
    {
        $this->resolver = $resolver;
    }

    #[Override]
    public function push(Job $job): string
    {
        $job = $this->normalize($job);

        // 没有解析器 → 保持内存队列行为，便于纯投递侧的单元测试
        if ($this->resolver === null || !$this->resolver->canResolve($job)) {
            return parent::push($job);
        }

        // 延迟任务在同步模式下也立即执行：本地调试不该真的等 10 分钟
        $handler = $this->resolver->resolve($job);
        $handler($job->incrementAttempts());

        return $job->id;
    }
}
