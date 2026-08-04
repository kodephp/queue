<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use Kode\Queue\Enum\Capability;
use Kode\Queue\Enum\Priority;
use Kode\Queue\Message\Job;
use Kode\Queue\Queue;
use Kode\Queue\QueueManager;
use Kode\Queue\Tests\Fixtures\SendMailJob;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

#[CoversClass(Queue::class)]
#[CoversClass(QueueManager::class)]
final class QueueTest extends TestCase
{
    private QueueManager $manager;

    private Queue $queue;

    protected function setUp(): void
    {
        $this->manager = QueueManager::make([
            'default' => 'memory',
            'connections' => [
                'memory' => ['driver' => 'memory', 'queue' => 'default'],
            ],
        ]);

        $this->queue = $this->manager->connection();
        SendMailJob::reset();
    }

    protected function tearDown(): void
    {
        $this->manager->close();
    }

    #[Test]
    public function 投递返回可排序的_ulid(): void
    {
        $id = $this->queue->push('demo.hello', ['name' => 'kode']);

        self::assertSame(26, \strlen($id));
        self::assertSame(1, $this->queue->size());
    }

    #[Test]
    public function peek_不消费任务(): void
    {
        $this->queue->push('demo.hello');

        self::assertInstanceOf(Job::class, $this->queue->peek());
        self::assertSame(1, $this->queue->size(), 'peek 之后任务必须还在队列里');
    }

    #[Test]
    public function 延迟任务在到期前不可见(): void
    {
        $this->queue->later(60, 'demo.later');

        self::assertSame(0, $this->queue->size());
        self::assertNull($this->queue->pop());
        self::assertSame(1, $this->queue->stats()->delayed);
    }

    #[Test]
    public function 高优先级任务排在队首(): void
    {
        $this->queue->push('demo.low');
        $this->queue->withPriority(Priority::High)->push('demo.high');

        self::assertSame('demo.high', $this->queue->peek()?->name);
    }

    #[Test]
    public function pop_会自增尝试次数并附带回执(): void
    {
        $this->queue->push('demo.hello');
        $reserved = $this->queue->pop();

        self::assertNotNull($reserved);
        self::assertSame(1, $reserved->job->attempts);
        self::assertNotSame('', (string) $reserved->receipt);
        self::assertSame(0, $this->queue->size(), '预留后不应再被其他 Worker 取到');
    }

    #[Test]
    public function ack_之后任务彻底消失(): void
    {
        $this->queue->push('demo.hello');
        $reserved = $this->queue->pop();

        self::assertNotNull($reserved);
        self::assertTrue($this->queue->ack($reserved));
        self::assertSame(0, $this->queue->size());
        self::assertSame(0, $this->queue->stats()->reserved);
    }

    #[Test]
    public function release_把任务放回队列且尝试次数累计(): void
    {
        $this->queue->push('demo.hello');

        $first = $this->queue->pop();
        self::assertNotNull($first);
        self::assertTrue($this->queue->release($first, 0));

        $second = $this->queue->pop();
        self::assertNotNull($second);
        self::assertSame(2, $second->job->attempts);
        self::assertSame($first->job->id, $second->job->id, '重投必须是同一条消息');
    }

    #[Test]
    public function 流式投递可覆盖各项参数(): void
    {
        $id = $this->queue->job('demo.fluent', ['a' => 1])
            ->onQueue('custom')
            ->priority(Priority::Low)
            ->retries(5)
            ->timeout(30)
            ->traceId('trace-abc')
            ->dispatch();

        self::assertSame(26, \strlen($id));
        self::assertSame(1, $this->queue->size('custom'));

        $job = $this->queue->peek('custom');
        self::assertNotNull($job);
        self::assertSame(5, $job->maxAttempts);
        self::assertSame(30, $job->timeout);
        self::assertSame(Priority::Low, $job->priority);
        self::assertSame('trace-abc', $job->traceId());
    }

    #[Test]
    public function dispatch_if_为假时不投递(): void
    {
        $this->queue->job('demo.maybe')->dispatchIf(false);

        self::assertSame(0, $this->queue->size());
    }

    #[Test]
    public function as_job_属性决定默认队列与优先级(): void
    {
        $this->queue->push(SendMailJob::class, ['to' => 'a@b.c']);

        self::assertSame(0, $this->queue->size(), '不应落在 default 队列');
        self::assertSame(1, $this->queue->size('mails'));

        $job = $this->queue->peek('mails');
        self::assertNotNull($job);
        self::assertSame('mail.send', $job->name);
        self::assertSame(2, $job->maxAttempts);
        self::assertSame(Priority::High, $job->priority);
    }

    #[Test]
    public function 批量投递返回等长的_id_列表(): void
    {
        $ids = $this->queue->bulk(['a', 'b', 'c'], ['shared' => 1]);

        self::assertCount(3, $ids);
        self::assertSame(3, $this->queue->size());
        self::assertSame(['shared' => 1], $this->queue->peek()?->payload);
    }

    #[Test]
    public function 批量投递支持每条自带负载与队列(): void
    {
        $ids = $this->queue->bulk([
            ['job' => 'mail.send', 'data' => ['to' => 'a@example.com']],
            ['job' => 'mail.send', 'data' => ['to' => 'b@example.com'], 'queue' => 'vip'],
            'sms.send',
        ], ['tenant' => 'acme'], queue: 'mails');

        self::assertCount(3, $ids);
        self::assertSame(2, $this->queue->size('mails'));
        self::assertSame(1, $this->queue->size('vip'));

        // 公共数据与条目自带数据合并，条目优先
        self::assertSame(
            ['tenant' => 'acme', 'to' => 'a@example.com'],
            $this->queue->peek('mails')?->payload,
        );
        self::assertSame('mail.send', $this->queue->peek('vip')?->name);
    }

    #[Test]
    public function 批量投递的数组条目缺少任务名时报错(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->queue->bulk([['data' => ['x' => 1]]]);
    }

    #[Test]
    public function composite_序列化器可用且能回读负载(): void
    {
        $queue = QueueManager::make([
            'default' => 'memory',
            'connections' => ['memory' => ['driver' => 'memory', 'serializer' => 'composite']],
        ])->default();

        $queue->push('t.ser', ['k' => 'v', 'n' => 42]);

        self::assertSame(['k' => 'v', 'n' => 42], $queue->peek()?->payload);
    }

    #[Test]
    public function 门面变更返回新实例且互不影响(): void
    {
        $other = $this->queue->onQueue('other');

        self::assertNotSame($this->queue, $other);
        self::assertSame('default', $this->queue->getDefaultQueue());
        self::assertSame('other', $other->getDefaultQueue());
    }

    #[Test]
    public function clear_清空指定队列(): void
    {
        $this->queue->bulk(['a', 'b', 'c']);

        self::assertSame(3, $this->queue->clear());
        self::assertSame(0, $this->queue->size());
    }

    #[Test]
    public function 能力探测如实反映驱动支持情况(): void
    {
        self::assertTrue($this->queue->supports(Capability::Delay));
        self::assertTrue($this->queue->supports(Capability::Ack));
    }

    #[Test]
    public function 诊断输出不泄露凭据(): void
    {
        $report = $this->manager->diagnose();

        self::assertArrayHasKey('memory', $report);
        self::assertTrue($report['memory']['available']);
        self::assertStringNotContainsString('"password":"', json_encode($report, \JSON_THROW_ON_ERROR));
    }

    #[Test]
    public function 同名连接复用同一实例(): void
    {
        self::assertSame($this->manager->connection('memory'), $this->manager->connection('memory'));
    }
}
