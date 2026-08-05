<?php

declare(strict_types=1);

namespace Kode\Queue;

use DateTimeImmutable;
use Kode\Queue\Contract\QueueInterface;
use Kode\Queue\Cron\CronExpression;

/**
 * 轻量级定时任务调度器（基于 delayed 投递，零外部依赖）。
 *
 * 与 Laravel Task Scheduling 思路一致，但不依赖任何框架：每个 cron 表达式在每次 tick 时
 * 计算「下一次运行时刻」，若落在守护进程的检查窗口内就投递进延迟队列，由 Worker 到点执行。
 * 因此「到点触发」的可靠性完全复用队列既有的至少一次投递与可见性超时机制，调度器本身无状态、
 * 可随 Worker 一起水平扩展（配合 {@see \Kode\Queue\Middleware\IdempotencyMiddleware} 去重即可避免重复执行）。
 *
 * ```php
 * $scheduler = Scheduler::for($queue)
 *     ->cron('0 9 * * *', ReportJob::class, ['type' => 'daily']) // 每天 9 点
 *     ->every(3600, HeartbeatJob::class);                        // 每隔 1 小时
 *
 * $scheduler->run(interval: 30.0); // 常驻守护，每 30s 检查一次
 * ```
 */
final class Scheduler
{
    /**
     * @var list<array{cron: CronExpression, job: string|object, data: array<string, mixed>, queue: ?string}>
     */
    private array $crons = [];

    /**
     * @var list<array{interval: int, job: string|object, data: array<string, mixed>, queue: ?string, last: ?int}>
     */
    private array $intervals = [];

    public function __construct(private readonly QueueInterface $queue)
    {
    }

    public static function for(QueueInterface $queue): self
    {
        return new self($queue);
    }

    /**
     * 注册一个 cron 风格定时任务。
     *
     * @param array<string, mixed> $data
     */
    public function cron(string $expression, string|object $job, array $data = [], ?string $queue = null): self
    {
        $this->crons[] = [CronExpression::parse($expression), $job, $data, $queue];

        return $this;
    }

    /**
     * 注册一个固定间隔任务（从注册时刻起每 $seconds 秒一次，首次立即触发）。
     *
     * @param array<string, mixed> $data
     */
    public function every(int $seconds, string|object $job, array $data = [], ?string $queue = null): self
    {
        if ($seconds <= 0) {
            throw new \InvalidArgumentException('every() 的间隔必须为正整数');
        }

        // last 初始为 null：首次 tick 立即触发，之后以传入的 $now 为时钟推进，
        // 不依赖系统时间，便于测试与容器内确定性触发。
        $this->intervals[] = [$seconds, $job, $data, $queue, null];

        return $this;
    }

    /**
     * 单次检查：把「运行点落在窗口内」的 cron 任务与「距上次投递已满间隔」的固定间隔任务投递进队列。
     *
     * @param float                 $window 守护进程的检查间隔（秒）。到期点在 [now, now+window] 内的 cron 任务会被投递。
     * @param DateTimeImmutable|null $now    基准时间，默认当前时间（主要用于测试注入）。
     * @return int 本次投递的任务数
     */
    public function tick(float $window = 60.0, ?DateTimeImmutable $now = null): int
    {
        $now ??= new DateTimeImmutable();
        $nowTs = $now->getTimestamp();
        $enqueued = 0;

        foreach ($this->crons as [$expr, $job, $data, $queue]) {
            $next = $expr->nextRun($now);
            $delay = $next->getTimestamp() - $nowTs;

            if ($delay <= $window) {
                $this->queue->later(max(0, $delay), $job, $data, $queue);
                $enqueued++;
            }
        }

        foreach ($this->intervals as &$def) {
            [$interval, $job, $data, $queue, $last] = $def;

            if ($last === null || $nowTs - $last >= $interval) {
                $this->queue->push($job, $data, $queue);
                $def[4] = $nowTs;
                $enqueued++;
            }
        }

        // 解除引用，避免后续修改污染
        unset($def);

        return $enqueued;
    }

    /**
     * 常驻守护：每 $interval 秒检查一次，直到进程被信号终止或达到 $maxIterations 上限。
     *
     * @param ?int $maxIterations 限制迭代次数（测试 / 一次性试运行用）
     */
    public function run(float $interval = 60.0, ?int $maxIterations = null): void
    {
        $iter = 0;

        while ($maxIterations === null || $iter < $maxIterations) {
            $this->tick($interval);
            $iter++;

            if ($maxIterations !== null && $iter >= $maxIterations) {
                break;
            }

            sleep((int) ceil($interval));
        }
    }
}
