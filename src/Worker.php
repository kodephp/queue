<?php

declare(strict_types=1);

namespace Kode\Queue;

use Kode\Queue\Config\WorkerOptions;
use Kode\Queue\Contract\EventDispatcherInterface;
use Kode\Queue\Contract\FailedJobStoreInterface;
use Kode\Queue\Contract\HandlerResolverInterface;
use Kode\Queue\Contract\QueueInterface;
use Kode\Queue\Enum\Capability;
use Kode\Queue\Event\JobFailed;
use Kode\Queue\Event\JobProcessed;
use Kode\Queue\Event\JobProcessing;
use Kode\Queue\Event\JobReleased;
use Kode\Queue\Event\WorkerPaused;
use Kode\Queue\Event\WorkerResumed;
use Kode\Queue\Event\WorkerStarting;
use Kode\Queue\Event\WorkerStopped;
use Kode\Queue\Exception\JobNotFoundException;
use Kode\Queue\Exception\JobTimeoutException;
use Kode\Queue\Integration\KodeContextBridge;
use Kode\Queue\Integration\KodeEventBridge;
use Kode\Queue\Integration\KodeRuntimeBridge;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Middleware\MetricsMiddleware;
use Kode\Queue\Support\WorkerSummary;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Throwable;

/**
 * 队列消费者。
 *
 * v1 根本没有 Worker——示例里让用户自己 `while (true) { $queue->pop(); }`，
 * 于是每个项目都要重新踩一遍同样的坑：任务抛异常整个循环挂掉、
 * `kill` 之后正在执行的任务被腰斩、内存涨到 OOM 被 OOM Killer 静默干掉、
 * 处理器死循环把队列堵死。
 *
 * v2 把这些全部内建：
 *
 * - **优雅退出**：`SIGTERM` / `SIGINT` / `SIGQUIT` 只置标志位，
 *   当前任务跑完才退出，绝不腰斩；
 * - **超时强杀**：用 `pcntl_alarm()` 真正执行 `Job::$timeout`，
 *   超时抛 {@see JobTimeoutException} 后照常走重试 / 死信流程；
 * - **自愈重启**：任务数 / 运行时长 / 内存三条红线任意触发即退出，
 *   交给 supervisor / systemd 拉起，天然规避内存泄漏累积；
 * - **失败留痕**：重试耗尽的任务落 {@see FailedJobStoreInterface}，可查询、可重放；
 * - **协程友好**：空转休眠走 {@see KodeRuntimeBridge}，
 *   在 Swoole / Swow 下让出调度权而不是阻塞整个线程；
 * - **暂停/恢复**：`SIGUSR2` 暂停消费（发布期止血），`SIGCONT` 恢复；
 * - **超时预留回收**：定期扫描崩溃 Worker 遗留的 reserved 任务，重新可见。
 *
 * ```php
 * $worker = Worker::for($queue, [
 *     'send.mail' => fn (array $data) => Mailer::send($data['to']),
 * ]);
 *
 * $summary = $worker->run(WorkerOptions::daemon('high', 'default'));
 * echo $summary;  // [kode-worker] limit | 处理 1000（成功 998 / 失败 2 ...）
 * ```
 */
final class Worker
{
    private readonly EventDispatcherInterface $events;

    /** 实际用于消费操作的队列句柄：未挂指标时等于 $queue，挂了指标后等于派生出的观测版 */
    private readonly QueueInterface $consuming;

    private bool $shouldStop = false;

    private bool $paused = false;

    private string $stopReason = WorkerSummary::REASON_STOPPED;

    /** 当前正在执行的任务（SIGALRM 处理器需要它来构造异常上下文） */
    private ?ReservedJob $current = null;

    private int $processed = 0;

    private int $failed = 0;

    private int $released = 0;

    private int $reclaimed = 0;

    private float $startedAt = 0.0;

    private float $lastReclaimAt = 0.0;

    private bool $signalsInstalled = false;

    public function __construct(
        private readonly QueueInterface $queue,
        private readonly HandlerResolverInterface $resolver = new HandlerResolver(),
        private readonly WorkerOptions $options = new WorkerOptions(),
        private readonly ?FailedJobStoreInterface $failedStore = null,
        ?EventDispatcherInterface $events = null,
        private readonly ?LoggerInterface $logger = null,
        private readonly ?MetricsMiddleware $metrics = null,
    ) {
        $this->events = $events
            ?? ($queue instanceof Queue ? $queue->getEvents() : KodeEventBridge::detect());

        // 派生一个挂了指标中间件的队列句柄；后续 pop/ack/release/fail 都走它，
        // 这样操作耗时与错误数被自动统计，而无需在热路径里散落埋点。
        $this->consuming = $metrics !== null ? $queue->withMiddleware($metrics) : $queue;
    }

    /**
     * 便捷构造：顺手把闭包处理器注册进去。
     *
     * @param array<string, callable|class-string> $handlers 任务名 => 处理器
     */
    public static function for(
        QueueInterface $queue,
        array $handlers = [],
        ?WorkerOptions $options = null,
        ?FailedJobStoreInterface $failedStore = null,
        ?MetricsMiddleware $metrics = null,
    ): self {
        return new self(
            queue: $queue,
            resolver: (new HandlerResolver())->registerMany($handlers),
            options: $options ?? new WorkerOptions(),
            failedStore: $failedStore,
            metrics: $metrics,
        );
    }

    // ------------------------------------------------------------------
    // 主循环
    // ------------------------------------------------------------------

    /**
     * 启动消费循环，直到触发任意退出条件。
     *
     * @param WorkerOptions|null $options 覆盖构造时的运行参数
     *
     * @throws Throwable `catchThrowable = false` 时，Error 会原样抛出（记录死信之后）
     */
    public function run(?WorkerOptions $options = null): WorkerSummary
    {
        $options ??= $this->options;

        $this->resetCounters();
        $this->installSignalHandlers($options);
        $this->events->dispatch(new WorkerStarting($options, getmypid() ?: 0));
        $this->log(LogLevel::INFO, sprintf(
            'Worker %s 启动，监听队列 [%s]，运行时 %s',
            $options->name,
            implode(', ', $options->queues),
            KodeRuntimeBridge::detect(),
        ));

        try {
            $this->loop($options);
        } catch (Throwable $e) {
            $this->stopReason = WorkerSummary::REASON_ERROR;
            $this->log(LogLevel::CRITICAL, 'Worker 异常终止：' . $e->getMessage(), [
                'exception' => $e::class,
            ]);
            $this->finish($options);

            throw $e;
        }

        return $this->finish($options);
    }

    /**
     * 只处理一个任务就返回，方便在自有事件循环 / 定时器里驱动。
     *
     * @return bool 是否真的处理了任务（队列为空时返回 false）
     */
    public function runOnce(?WorkerOptions $options = null): bool
    {
        $options ??= $this->options;

        if ($this->startedAt === 0.0) {
            $this->startedAt = microtime(true);
        }

        $reserved = $this->reserveNext($options);

        if ($reserved === null) {
            return false;
        }

        $this->process($reserved, $options);

        return true;
    }

    /**
     * 把队列跑空后返回，不常驻（CI / 一次性补数场景）。
     */
    public function drain(?string ...$queues): WorkerSummary
    {
        $names = array_values(array_filter($queues, \is_string(...)));

        return $this->run(
            $names === []
                ? WorkerOptions::batch(...$this->options->queues)->withName($this->options->name)
                : WorkerOptions::batch(...$names)->withName($this->options->name),
        );
    }

    // ------------------------------------------------------------------
    // 生命周期控制
    // ------------------------------------------------------------------

    /** 请求优雅退出（当前任务执行完毕后生效） */
    public function stop(string $reason = WorkerSummary::REASON_STOPPED): void
    {
        $this->shouldStop = true;
        $this->stopReason = $reason;
    }

    /** 暂停消费：已预留的任务照常跑完，之后只空转不取新任务 */
    public function pause(): void
    {
        if ($this->paused) {
            return;
        }

        $this->paused = true;
        $this->events->dispatch(new WorkerPaused($this->options->name, 'explicit'));
    }

    public function resume(): void
    {
        if (!$this->paused) {
            return;
        }

        $this->paused = false;
        $this->events->dispatch(new WorkerResumed($this->options->name, 'explicit'));
    }

    public function isPaused(): bool
    {
        return $this->paused;
    }

    /** 当前正在执行的任务（监控 / 心跳上报用） */
    public function currentJob(): ?ReservedJob
    {
        return $this->current;
    }

    /** 实时计数快照 */
    public function snapshot(): WorkerSummary
    {
        return $this->buildSummary($this->options, 'running');
    }

    public function getResolver(): HandlerResolverInterface
    {
        return $this->resolver;
    }

    /** 指标快照（未启用指标中间件时返回空数组） */
    public function metricsSnapshot(): array
    {
        return $this->metrics?->snapshot() ?? [];
    }

    // ------------------------------------------------------------------
    // 内部：循环与取任务
    // ------------------------------------------------------------------

    private function loop(WorkerOptions $options): void
    {
        while (true) {
            $this->pumpSignals();

            if ($this->shouldStop) {
                return;
            }

            if ($this->paused) {
                $this->rest(max(0.2, $options->sleep));

                continue;
            }

            $this->reclaimExpired($options);

            $reserved = $this->reserveNext($options);

            if ($reserved === null) {
                if ($options->stopWhenEmpty) {
                    $this->stopReason = WorkerSummary::REASON_EMPTY;

                    return;
                }

                $this->rest($options->sleep);

                if ($this->reachedLimit($options)) {
                    return;
                }

                continue;
            }

            $this->process($reserved, $options);

            if ($this->reachedLimit($options)) {
                return;
            }
        }
    }

    /**
     * 按 `queues` 的声明顺序做严格优先级取任务。
     *
     * 阻塞拉取只用在最后一个队列上——否则会卡在低优先级队列的长轮询里，
     * 眼睁睁看着高优先级队列堆积。
     */
    private function reserveNext(WorkerOptions $options): ?ReservedJob
    {
        $queues = $options->queues === [] ? [$this->consuming->getDefaultQueue()] : $options->queues;
        $last = \count($queues) - 1;
        $canBlock = $options->blockTimeout > 0.0 && $this->consuming->supports(Capability::Blocking);

        foreach ($queues as $index => $name) {
            $timeout = ($canBlock && $index === $last) ? $options->blockTimeout : 0.0;

            try {
                $reserved = $this->consuming->pop($name, $timeout);
            } catch (Throwable $e) {
                // 后端瞬时抖动不该让 Worker 直接死掉：记日志、退避、下一轮再来
                $this->log(LogLevel::ERROR, sprintf('从队列 %s 取任务失败：%s', $name, $e->getMessage()), [
                    'exception' => $e::class,
                ]);
                $this->rest(max(1.0, $options->sleep));

                return null;
            }

            if ($reserved !== null) {
                return $reserved;
            }
        }

        return null;
    }

    /**
     * 定期回收超时未确认的预留任务（上一个 Worker 崩溃留下的孤儿）。
     */
    private function reclaimExpired(WorkerOptions $options): void
    {
        if ($options->reclaimEvery <= 0.0) {
            return;
        }

        $now = microtime(true);

        if ($now - $this->lastReclaimAt < $options->reclaimEvery) {
            return;
        }

        $this->lastReclaimAt = $now;

        // 惊群防护：多个 Worker 同时回收孤儿预留任务时，给每个进程加一点随机错峰，
        // 避免它们在同一毫秒里一起冲向后端。本地内存驱动是单进程的，这个抖动没有意义但不影响正确性。
        if ($options->reclaimJitter > 0.0) {
            $wait = $options->reclaimJitter * (random_int(0, 1000) / 1000);
            $this->rest($wait);
        }

        $driver = $this->consuming->getDriver();

        foreach ($options->queues as $name) {
            try {
                $count = $driver->reclaimExpired($name);
            } catch (Throwable $e) {
                $this->log(LogLevel::WARNING, sprintf('回收队列 %s 的超时任务失败：%s', $name, $e->getMessage()));

                continue;
            }

            if ($count > 0) {
                $this->reclaimed += $count;
                $this->log(LogLevel::NOTICE, sprintf('队列 %s 回收了 %d 个超时预留任务', $name, $count));
            }
        }
    }

    // ------------------------------------------------------------------
    // 内部：执行单个任务
    // ------------------------------------------------------------------

    private function process(ReservedJob $reserved, WorkerOptions $options): void
    {
        $job = $reserved->job;
        $this->current = $reserved;
        $this->processed++;
        $startedAt = microtime(true);

        $this->events->dispatch(new JobProcessing($reserved, $options->name));
        $this->log(LogLevel::INFO, sprintf('开始处理 %s（第 %d 次尝试）', (string) $job, max(1, $job->attempts)));

        try {
            // 任务级上下文隔离：常驻 worker 顺序消费时，处理器经 Context::set
            // 写入的业务键（含异常路径）必须在任务结束后整体回滚，否则会脏读
            // 给下一个任务。scoped() 取 fork 语义——继承 ContextMiddleware 在
            // pop 时导入的 trace_id / tenant_id，但处理器新增的键不外泄。
            $result = KodeContextBridge::scoped(fn (): mixed => $this->invoke($job, $options));

            $this->consuming->ack($reserved);

            $durationMs = (microtime(true) - $startedAt) * 1000;
            $this->events->dispatch(new JobProcessed($reserved, $options->name, $durationMs, $result));
            $this->log(LogLevel::INFO, sprintf('完成 %s，耗时 %.1fms', (string) $job, $durationMs));
        } catch (JobNotFoundException $e) {
            // 找不到处理器时重试毫无意义（代码没部署上去，重试一百次也一样），直接进死信
            $this->bury($reserved, $e, $options);
        } catch (Throwable $e) {
            // Error（内存耗尽、类型错误……）通常意味着进程状态已不可信：
            // 记录死信后原样抛出，交给进程管理器重启，而不是带病继续消费
            if (!$options->catchThrowable && !$e instanceof \Exception) {
                $this->bury($reserved, $e, $options);

                throw $e;
            }

            $this->retryOrBury($reserved, $e, $options);
        } finally {
            $this->current = null;
        }
    }

    /**
     * 解析并执行处理器，`Job::$timeout` 优先用 `pcntl_alarm()` 硬中断兑现。
     *
     * 当进程没有 pcntl（常驻 Swoole/Swow 协程、或没编译 ext-pcntl）时，硬超时不可用，
     * 退化为**软超时兜底**：把任务跑完再比对耗时，一旦超限就按超时失败。
     * 软超时无法中途打断执行，但至少保证「跑飞的任务」不会被误判成功、带着副作用安然过关。
     */
    private function invoke(Job $job, WorkerOptions $options): mixed
    {
        $handler = $this->resolver->resolve($job);
        $timeout = $job->timeout;

        if ($timeout <= 0) {
            return $handler($job);
        }

        if ($this->canUseAlarm()) {
            pcntl_alarm($timeout);

            try {
                return $handler($job);
            } finally {
                pcntl_alarm(0);
            }
        }

        // 软超时兜底：硬中断用不了时，至少在处理结束后拦下明显超时的任务。
        $startedAt = microtime(true);
        $result = $handler($job);
        $elapsed = (int) ceil(microtime(true) - $startedAt);

        if ($elapsed > $timeout) {
            throw JobTimeoutException::for($job, $timeout);
        }

        return $result;
    }

    private function retryOrBury(ReservedJob $reserved, Throwable $e, WorkerOptions $options): void
    {
        $job = $reserved->job;

        // 不可重试异常：重试多少次都不会好（例如驱动根本不支持某能力），直接落死信，
        // 避免「重试风暴」把队列和后端一起拖死。
        foreach ($options->nonRetryableExceptions as $type) {
            if ($e instanceof $type) {
                $this->bury($reserved, $e, $options);

                return;
            }
        }

        $limit = $this->retryLimit($job, $options);

        if ($limit > 0 && $job->attempts >= $limit) {
            $this->bury($reserved, $e, $options);

            return;
        }

        $delay = $this->retryDelay($job, $options);

        try {
            $this->consuming->release($reserved, $delay);
        } catch (Throwable $releaseError) {
            // 连重投都失败，只能落死信兜底，否则这条消息就真丢了
            $this->log(LogLevel::ERROR, '重投失败，转入死信：' . $releaseError->getMessage());
            $this->bury($reserved, $e, $options);

            return;
        }

        $this->released++;
        $this->events->dispatch(new JobReleased($reserved, $e, $options->name, $delay));
        $this->events->dispatch(new JobFailed($reserved, $e, $options->name, willRetry: true));
        $this->log(LogLevel::WARNING, sprintf(
            '%s 第 %d 次失败：%s，%d 秒后重试',
            (string) $job,
            max(1, $job->attempts),
            $e->getMessage(),
            $delay,
        ), ['exception' => $e::class]);
    }

    /**
     * 终态失败：先落死信库（哪怕后端 fail 失败，记录也在），再通知后端。
     */
    private function bury(ReservedJob $reserved, Throwable $e, WorkerOptions $options): void
    {
        $this->failed++;

        try {
            $this->failedStore?->store($reserved->job, $e, $this->connectionName());
        } catch (Throwable $storeError) {
            $this->log(LogLevel::CRITICAL, '死信落库失败：' . $storeError->getMessage(), [
                'job_id' => $reserved->job->id,
                'original' => $e->getMessage(),
            ]);
        }

        try {
            $this->consuming->fail($reserved, $e);
        } catch (Throwable $failError) {
            $this->log(LogLevel::ERROR, '标记任务失败时出错：' . $failError->getMessage());
        }

        $this->events->dispatch(new JobFailed($reserved, $e, $options->name, willRetry: false));
        $this->log(LogLevel::ERROR, sprintf(
            '%s 最终失败（已尝试 %d 次）：%s',
            (string) $reserved->job,
            max(1, $reserved->job->attempts),
            $e->getMessage(),
        ), ['exception' => $e::class]);
    }

    /**
     * 允许的最大尝试次数。
     *
     * 规则：**任务自带值优先**，Worker 参数只作全局上限——两者都设置时取更严的那个，
     * 任务声明「无限重试」（maxAttempts <= 0）时才由 Worker 兜底。
     * 返回 0 表示不封顶。
     */
    private function retryLimit(Job $job, WorkerOptions $options): int
    {
        $ceiling = max(0, $options->maxAttempts);

        return match (true) {
            $job->maxAttempts <= 0 => $ceiling,
            $ceiling <= 0 => $job->maxAttempts,
            default => min($job->maxAttempts, $ceiling),
        };
    }

    /**
     * 下次重试的等待秒数：Worker 参数是「覆盖」而非「默认」，
     * 不设置时完全沿用任务自己声明的退避策略。
     */
    private function retryDelay(Job $job, WorkerOptions $options): int
    {
        $strategy = $options->backoff ?? $job->backoff;
        $base = $options->backoffBase > 0 ? $options->backoffBase : $job->backoffBase;

        return $strategy->delayFor(max(1, $job->attempts), $base);
    }

    // ------------------------------------------------------------------
    // 内部：信号与休眠
    // ------------------------------------------------------------------

    private function installSignalHandlers(WorkerOptions $options): void
    {
        if ($this->signalsInstalled || !KodeRuntimeBridge::supportsSignals()) {
            return;
        }

        pcntl_async_signals(true);

        $stop = function (int $signal): void {
            $this->shouldStop = true;
            $this->stopReason = WorkerSummary::REASON_SIGNAL;
            $this->log(LogLevel::NOTICE, sprintf('收到信号 %d，当前任务完成后退出', $signal));
        };

        $signals = array_unique([\SIGTERM, \SIGINT, \SIGQUIT, $options->restartSignal]);

        foreach ($signals as $signal) {
            pcntl_signal($signal, $stop);
        }

        // 发布期止血：不重启进程也能暂停消费
        pcntl_signal(\SIGUSR2, function (): void {
            $this->paused = !$this->paused;
            $this->log(LogLevel::NOTICE, $this->paused ? 'Worker 已暂停消费' : 'Worker 已恢复消费');
            $this->events->dispatch(
                $this->paused
                    ? new WorkerPaused($this->options->name, 'SIGUSR2')
                    : new WorkerResumed($this->options->name, 'SIGUSR2'),
            );
        });
        pcntl_signal(\SIGCONT, function (): void {
            if ($this->paused) {
                $this->paused = false;
                $this->events->dispatch(new WorkerResumed($this->options->name, 'SIGCONT'));
            }
        });

        // 任务超时：从执行现场直接抛异常，走正常的重试 / 死信路径
        pcntl_signal(\SIGALRM, function (): void {
            $job = $this->current?->job;

            throw $job !== null
                ? JobTimeoutException::for($job, $job->timeout)
                : JobTimeoutException::unknown(0);
        });

        $this->signalsInstalled = true;
    }

    private function pumpSignals(): void
    {
        // async_signals 打开时是空操作；某些 SAPI 下仍需手动泵一次
        if (\function_exists('pcntl_signal_dispatch') && $this->signalsInstalled) {
            pcntl_signal_dispatch();
        }
    }

    private function canUseAlarm(): bool
    {
        return $this->signalsInstalled && \function_exists('pcntl_alarm');
    }

    /** 协程友好休眠 */
    private function rest(float $seconds): void
    {
        if ($seconds > 0.0) {
            KodeRuntimeBridge::sleep($seconds);
        }

        $this->pumpSignals();
    }

    // ------------------------------------------------------------------
    // 内部：状态与收尾
    // ------------------------------------------------------------------

    private function reachedLimit(WorkerOptions $options): bool
    {
        if (!$options->shouldStop($this->processed, $this->startedAt)) {
            return false;
        }

        $this->stopReason = WorkerSummary::REASON_LIMIT;

        return true;
    }

    private function resetCounters(): void
    {
        $this->shouldStop = false;
        $this->paused = false;
        $this->stopReason = WorkerSummary::REASON_STOPPED;
        $this->processed = 0;
        $this->failed = 0;
        $this->released = 0;
        $this->reclaimed = 0;
        $this->startedAt = microtime(true);
        $this->lastReclaimAt = 0.0;
    }

    private function finish(WorkerOptions $options): WorkerSummary
    {
        $summary = $this->buildSummary($options, $this->stopReason);

        $this->events->dispatch(new WorkerStopped(
            worker: $options->name,
            reason: $summary->reason,
            processed: $summary->processed,
            failed: $summary->failed,
            uptimeSeconds: $summary->uptimeSeconds,
        ));

        $this->log(LogLevel::INFO, (string) $summary);

        if ($this->metrics !== null) {
            $this->log(LogLevel::INFO, "本轮指标采集：\n" . $this->metrics->summary());
        }

        return $summary;
    }

    private function buildSummary(WorkerOptions $options, string $reason): WorkerSummary
    {
        return new WorkerSummary(
            worker: $options->name,
            reason: $reason,
            processed: $this->processed,
            failed: $this->failed,
            released: $this->released,
            reclaimed: $this->reclaimed,
            uptimeSeconds: $this->startedAt > 0.0 ? microtime(true) - $this->startedAt : 0.0,
            peakMemoryBytes: memory_get_peak_usage(true),
        );
    }

    private function connectionName(): string
    {
        return $this->queue instanceof Queue ? $this->queue->getConnectionName() : 'default';
    }

    /**
     * @param array<string, mixed> $context
     */
    private function log(string $level, string $message, array $context = []): void
    {
        $this->logger?->log($level, '[kode/queue] ' . $message, $context);
    }
}
