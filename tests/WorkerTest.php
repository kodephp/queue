<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use Kode\Queue\Config\WorkerOptions;
use Kode\Queue\Contract\EventDispatcherInterface;
use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Event\JobFailed;
use Kode\Queue\Event\JobProcessed;
use Kode\Queue\Event\JobProcessing;
use Kode\Queue\Event\WorkerStarting;
use Kode\Queue\Event\WorkerStopped;
use Kode\Queue\Failed\ArrayFailedJobStore;
use Kode\Queue\HandlerResolver;
use Kode\Queue\Message\Job;
use Kode\Queue\Queue;
use Kode\Queue\QueueManager;
use Kode\Queue\Support\WorkerSummary;
use Kode\Queue\Tests\Fixtures\SendMailJob;
use Kode\Queue\Worker;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(Worker::class)]
#[CoversClass(WorkerSummary::class)]
#[CoversClass(WorkerOptions::class)]
final class WorkerTest extends TestCase
{
    private QueueManager $manager;

    private Queue $queue;

    private ArrayFailedJobStore $failed;

    protected function setUp(): void
    {
        $this->manager = QueueManager::make([
            'default' => 'memory',
            'connections' => ['memory' => ['driver' => 'memory', 'queue' => 'default']],
        ]);

        $this->queue = $this->manager->connection();
        $this->failed = new ArrayFailedJobStore();
        SendMailJob::reset();
    }

    protected function tearDown(): void
    {
        $this->manager->close();
    }

    #[Test]
    public function 批处理模式跑空队列后退出(): void
    {
        $seen = [];
        $worker = $this->worker([
            'demo.ok' => function (array $data) use (&$seen): string {
                $seen[] = $data['n'];

                return 'ok';
            },
        ]);

        for ($i = 1; $i <= 5; $i++) {
            $this->queue->push('demo.ok', ['n' => $i]);
        }

        $summary = $worker->run(WorkerOptions::batch('default'));

        self::assertSame(5, $summary->processed);
        self::assertSame(0, $summary->failed);
        self::assertSame(WorkerSummary::REASON_EMPTY, $summary->reason);
        self::assertSame([1, 2, 3, 4, 5], $seen, '必须按 FIFO 顺序消费');
        self::assertTrue($summary->isClean());
        self::assertSame(0, $this->queue->size());
    }

    #[Test]
    public function 失败任务重试到上限后进入死信(): void
    {
        $attempts = 0;
        $worker = $this->worker([
            'demo.boom' => function () use (&$attempts): never {
                $attempts++;

                throw new RuntimeException('故意炸一个');
            },
        ]);

        $this->queue->pushJob(Job::create(
            name: 'demo.boom',
            maxAttempts: 3,
            timeout: 0,
            backoff: BackoffStrategy::Fixed,
            backoffBase: 0,
        ));

        $summary = $worker->run(WorkerOptions::batch('default'));

        self::assertSame(3, $attempts, '应当尝试 maxAttempts 次');
        self::assertSame(2, $summary->released, '前两次失败是重投');
        self::assertSame(1, $summary->failed);
        self::assertFalse($summary->isClean());
        self::assertSame(1, $this->failed->count());

        $record = $this->failed->all()[0];
        self::assertInstanceOf(Job::class, $record['job']);
        self::assertStringContainsString('故意炸一个', $record['exception']);
        self::assertSame('memory', $record['connection']);
    }

    #[Test]
    public function worker_的_max_attempts_只作全局上限(): void
    {
        $attempts = 0;
        $worker = $this->worker([
            'demo.boom' => function () use (&$attempts): never {
                $attempts++;

                throw new RuntimeException('boom');
            },
        ]);

        // 任务声明重试 10 次，Worker 全局上限 2 → 取更严的 2
        $this->queue->pushJob(Job::create(
            name: 'demo.boom',
            maxAttempts: 10,
            timeout: 0,
            backoff: BackoffStrategy::Fixed,
            backoffBase: 0,
        ));

        $worker->run(new WorkerOptions(
            queues: ['default'],
            sleep: 0.0,
            blockTimeout: 0.0,
            maxAttempts: 2,
            stopWhenEmpty: true,
            reclaimEvery: 0.0,
        ));

        self::assertSame(2, $attempts);
        self::assertSame(1, $this->failed->count());
    }

    #[Test]
    public function 找不到处理器时不重试直接进死信(): void
    {
        $this->queue->push('demo.missing');

        $summary = $this->worker([])->run(WorkerOptions::batch('default'));

        self::assertSame(1, $summary->processed);
        self::assertSame(0, $summary->released, '处理器缺失重试无意义');
        self::assertSame(1, $summary->failed);
        self::assertSame(1, $this->failed->count());
    }

    #[Test]
    public function 达到_max_jobs_上限后停止(): void
    {
        $this->queue->bulk(['demo.ok', 'demo.ok', 'demo.ok', 'demo.ok']);

        $summary = $this->worker(['demo.ok' => static fn (): bool => true])->run(new WorkerOptions(
            queues: ['default'],
            sleep: 0.0,
            blockTimeout: 0.0,
            maxJobs: 2,
            reclaimEvery: 0.0,
        ));

        self::assertSame(2, $summary->processed);
        self::assertSame(WorkerSummary::REASON_LIMIT, $summary->reason);
        self::assertSame(2, $this->queue->size(), '剩余任务应留在队列里');
    }

    #[Test]
    public function 按声明顺序做严格优先级消费(): void
    {
        $order = [];
        $handler = static function (array $data) use (&$order): void {
            $order[] = $data['tag'];
        };

        $this->queue->push('demo.ok', ['tag' => 'low'], 'low');
        $this->queue->push('demo.ok', ['tag' => 'high'], 'high');

        $this->worker(['demo.ok' => $handler])->run(WorkerOptions::batch('high', 'low'));

        self::assertSame(['high', 'low'], $order);
    }

    #[Test]
    public function run_once_只处理一个任务(): void
    {
        $this->queue->bulk(['demo.ok', 'demo.ok']);
        $worker = $this->worker(['demo.ok' => static fn (): bool => true]);

        self::assertTrue($worker->runOnce(WorkerOptions::batch('default')));
        self::assertSame(1, $this->queue->size());

        self::assertTrue($worker->runOnce(WorkerOptions::batch('default')));
        self::assertFalse($worker->runOnce(WorkerOptions::batch('default')), '队列空时应返回 false');
    }

    #[Test]
    public function stop_请求会让循环优雅退出(): void
    {
        $worker = null;
        $processed = 0;

        $worker = $this->worker([
            'demo.ok' => function () use (&$worker, &$processed): void {
                $processed++;
                $worker?->stop();
            },
        ]);

        $this->queue->bulk(['demo.ok', 'demo.ok', 'demo.ok']);

        $summary = $worker->run(new WorkerOptions(
            queues: ['default'],
            sleep: 0.0,
            blockTimeout: 0.0,
            reclaimEvery: 0.0,
        ));

        self::assertSame(1, $processed, '当前任务跑完就退出，不腰斩也不多做');
        self::assertSame(WorkerSummary::REASON_STOPPED, $summary->reason);
        self::assertSame(2, $this->queue->size());
    }

    #[Test]
    public function 全生命周期事件都会被派发(): void
    {
        $events = new RecordingDispatcher();

        $this->queue->push('demo.ok');
        $this->queue->pushJob(Job::create('demo.boom', maxAttempts: 1, timeout: 0));

        $worker = new Worker(
            queue: $this->queue,
            resolver: (new HandlerResolver())->registerMany([
                'demo.ok' => static fn (): string => 'ok',
                'demo.boom' => static fn (): never => throw new RuntimeException('x'),
            ]),
            options: WorkerOptions::batch('default'),
            failedStore: $this->failed,
            events: $events,
        );

        $worker->run();

        self::assertSame(1, $events->countOf(WorkerStarting::class));
        self::assertSame(1, $events->countOf(WorkerStopped::class));
        self::assertSame(2, $events->countOf(JobProcessing::class));
        self::assertSame(1, $events->countOf(JobProcessed::class));
        self::assertSame(1, $events->countOf(JobFailed::class));

        $failed = $events->firstOf(JobFailed::class);
        self::assertInstanceOf(JobFailed::class, $failed);
        self::assertFalse($failed->willRetry);
    }

    #[Test]
    public function 属性任务类可被自动解析执行(): void
    {
        $this->queue->push(SendMailJob::class, ['to' => 'a@b.c']);

        $worker = new Worker(
            queue: $this->queue,
            resolver: (new HandlerResolver())->registerJobClass(SendMailJob::class),
            options: WorkerOptions::batch('mails'),
            failedStore: $this->failed,
        );

        $summary = $worker->run();

        self::assertSame(1, $summary->processed);
        self::assertSame(0, $summary->failed);
        self::assertSame(['a@b.c'], SendMailJob::$sent);
    }

    #[Test]
    public function 处理器可以混用_名称映射_与_纯类名列表(): void
    {
        $this->queue->push('mail.plain', ['to' => 'plain@b.c']);
        $this->queue->push(SendMailJob::class, ['to' => 'attr@b.c']);

        $seen = [];
        $summary = Worker::for(
            queue: $this->queue,
            handlers: [
                'mail.plain' => static function (array $payload) use (&$seen): void {
                    $seen[] = $payload['to'];
                },
                SendMailJob::class,   // 列表项：任务名从 #[AsJob] 读
            ],
            options: WorkerOptions::batch('default', 'mails'),
            failedStore: $this->failed,
        )->run();

        self::assertSame(2, $summary->processed);
        self::assertSame(0, $summary->failed);
        self::assertSame(['plain@b.c'], $seen);
        self::assertSame(['attr@b.c'], SendMailJob::$sent);
    }

    #[Test]
    public function 列表形式的处理器条目必须是任务类名(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new HandlerResolver())->registerMany([static fn (): null => null]);
    }

    #[Test]
    public function 摘要能算出吞吐与可读文本(): void
    {
        $summary = new WorkerSummary(
            worker: 'w1',
            reason: WorkerSummary::REASON_LIMIT,
            processed: 10,
            failed: 2,
            uptimeSeconds: 2.0,
        );

        self::assertSame(8, $summary->succeeded());
        self::assertSame(5.0, $summary->throughput());
        self::assertStringContainsString('处理 10', (string) $summary);
        self::assertSame(10, $summary->toArray()['processed']);
    }

    #[Test]
    public function 暂停后不再取新任务(): void
    {
        $worker = $this->worker(['demo.ok' => static fn (): bool => true]);
        $worker->pause();

        self::assertTrue($worker->isPaused());

        $worker->resume();
        self::assertFalse($worker->isPaused());
    }

    /**
     * @param array<string, callable> $handlers
     */
    private function worker(array $handlers): Worker
    {
        return new Worker(
            queue: $this->queue,
            resolver: (new HandlerResolver())->registerMany($handlers),
            options: WorkerOptions::batch('default'),
            failedStore: $this->failed,
        );
    }
}

/**
 * 记录所有派发过的事件，供断言使用。
 */
final class RecordingDispatcher implements EventDispatcherInterface
{
    /** @var list<object> */
    public array $events = [];

    public function dispatch(object $event): object
    {
        $this->events[] = $event;

        return $event;
    }

    public function countOf(string $class): int
    {
        return \count(array_filter($this->events, static fn (object $e): bool => $e instanceof $class));
    }

    public function firstOf(string $class): ?object
    {
        foreach ($this->events as $event) {
            if ($event instanceof $class) {
                return $event;
            }
        }

        return null;
    }
}
