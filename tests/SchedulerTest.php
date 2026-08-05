<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use DateTimeImmutable;
use Kode\Queue\Cron\CronExpression;
use Kode\Queue\Cron\CronParseException;
use Kode\Queue\Queue;
use Kode\Queue\QueueManager;
use Kode\Queue\Scheduler;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

#[CoversClass(CronExpression::class)]
#[CoversClass(Scheduler::class)]
final class SchedulerTest extends TestCase
{
    private QueueManager $manager;

    private Queue $queue;

    protected function setUp(): void
    {
        $this->manager = QueueManager::make([
            'default' => 'memory',
            'connections' => ['memory' => ['driver' => 'memory', 'queue' => 'default']],
        ]);

        $this->queue = $this->manager->connection();
    }

    protected function tearDown(): void
    {
        $this->manager->close();
    }

    // ------------------------------------------------------------------
    // CronExpression
    // ------------------------------------------------------------------

    #[Test]
    public function 每五分钟的表达式正确推进到下一个五分位(): void
    {
        $next = CronExpression::parse('*/5 * * * *')->nextRun(new DateTimeImmutable('2026-08-04 10:02:30'));

        self::assertSame('2026-08-04 10:05', $next->format('Y-m-d H:i'));
    }

    #[Test]
    public function 每天九点的表达式跨天触发(): void
    {
        $next = CronExpression::parse('0 9 * * *')->nextRun(new DateTimeImmutable('2026-08-04 10:00:00'));

        self::assertSame('2026-08-05 09:00', $next->format('Y-m-d H:i'));
    }

    #[Test]
    public function 元旦表达式跨年触发(): void
    {
        $next = CronExpression::parse('0 0 1 1 *')->nextRun(new DateTimeImmutable('2026-08-04 10:00:00'));

        self::assertSame('2027-01-01 00:00', $next->format('Y-m-d H:i'));
    }

    #[Test]
    public function 周字段只匹配周日(): void
    {
        // 2026-08-04 是周二，下一个周日应是 08-09
        $next = CronExpression::parse('0 0 * * 0')->nextRun(new DateTimeImmutable('2026-08-04 10:00:00'));

        self::assertSame('2026-08-09 00:00', $next->format('Y-m-d H:i'));
    }

    #[Test]
    public function 工作日九点跳过周末(): void
    {
        // 2026-08-08 是周六，下一个工作日 9 点是周一 08-10
        $next = CronExpression::parse('0 9 * * 1-5')->nextRun(new DateTimeImmutable('2026-08-08 10:00:00'));

        self::assertSame('2026-08-10 09:00', $next->format('Y-m-d H:i'));
    }

    #[Test]
    public function 非法表达式抛出解析异常(): void
    {
        $this->expectException(CronParseException::class);
        CronExpression::parse('这不是 cron');
    }

    #[Test]
    public function 字段数量错误也会抛异常(): void
    {
        $this->expectException(CronParseException::class);
        CronExpression::parse('0 9 * * * *');
    }

    // ------------------------------------------------------------------
    // Scheduler
    // ------------------------------------------------------------------

    #[Test]
    public function cron_任务在窗口内会被投递为延迟任务(): void
    {
        $now = new DateTimeImmutable('2026-08-04 10:02:30');
        $scheduler = Scheduler::for($this->queue)->cron('* * * * *', 'demo.cron');

        $enqueued = $scheduler->tick(60.0, $now);

        self::assertSame(1, $enqueued);
        // 延迟投递：就绪队列为空，延迟区有 1 条
        self::assertSame(0, $this->queue->size());
        self::assertSame(1, $this->queue->stats()->delayed);
    }

    #[Test]
    public function cron_运行点超出窗口时不投递(): void
    {
        $now = new DateTimeImmutable('2026-08-04 10:00:00');
        $scheduler = Scheduler::for($this->queue)->cron('0 9 * * *', 'demo.cron');

        self::assertSame(0, $scheduler->tick(60.0, $now), '9 点任务在 10 点触发，远超出 60s 窗口');
        self::assertSame(0, $this->queue->stats()->delayed);
    }

    #[Test]
    public function every_首次_tick_立即投递且不会重复(): void
    {
        $now = new DateTimeImmutable('2026-08-04 10:00:00');
        $scheduler = Scheduler::for($this->queue)->every(3600, 'demo.every');

        self::assertSame(1, $scheduler->tick(60.0, $now), '首次 tick 立即触发');
        // 同一秒内再次 tick 不应重复投递
        self::assertSame(0, $scheduler->tick(60.0, $now), '间隔未满，不应重复');
        self::assertSame(1, $this->queue->size(), '队列里只有 1 条');
    }
}
