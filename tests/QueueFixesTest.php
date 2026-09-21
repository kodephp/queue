<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use Kode\Queue\Config\QueueConfig;
use Kode\Queue\Config\WorkerOptions;
use Kode\Queue\Enum\JobStatus;
use Kode\Queue\Integration\KodeContextBridge;
use Kode\Queue\Message\Job;
use Kode\Queue\Support\IdGenerator;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

/**
 * v2.3.0 回归：死信重试清零、上下文导入清场、
 * env/CLI 假值解析、fork 后重新播种。
 */
final class QueueFixesTest extends TestCase
{
    protected function tearDown(): void
    {
        KodeContextBridge::reset();
        unset($_ENV['QUEUE_RETRY_AFTER']);
    }

    #[Test]
    public function resetAttemptsClearsCounterForManualRetry(): void
    {
        $job = Job::create('demo.job')
            ->incrementAttempts()
            ->incrementAttempts();

        $this->assertSame(2, $job->attempts);

        $reset = $job->resetAttempts();

        $this->assertSame(0, $reset->attempts);
        $this->assertSame(2, $job->attempts, '不可变语义：原对象不受影响');
    }

    #[Test]
    public function failedRetryResetsAttemptsAndStatus(): void
    {
        $job = Job::create('demo.job')
            ->incrementAttempts()
            ->withStatus(JobStatus::Failed);

        $again = $job->resetAttempts()->withStatus(JobStatus::Pending);

        $this->assertSame(0, $again->attempts);
        $this->assertSame(JobStatus::Pending, $again->status);
    }

    #[Test]
    public function contextImportClearsStaleScopeKeys(): void
    {
        KodeContextBridge::set('trace_id', 'old-trace');
        KodeContextBridge::set('request_id', 'old-request');

        KodeContextBridge::import(['trace_id' => 'new-trace'], ['trace_id', 'request_id']);

        $this->assertSame('new-trace', KodeContextBridge::get('trace_id'));
        $this->assertNull(KodeContextBridge::get('request_id'), '本请求未携带的旧键必须被清空，不得跨任务泄漏');
    }

    #[Test]
    public function workerOptionsParseStringBooleansFromCli(): void
    {
        $off = WorkerOptions::fromArray([
            'stop_when_empty' => 'false',
            'catch_throwable' => 'off',
        ]);
        $this->assertFalse($off->stopWhenEmpty);
        $this->assertFalse($off->catchThrowable);

        $on = WorkerOptions::fromArray([
            'stop_when_empty' => 'true',
            'catch_throwable' => 'yes',
        ]);
        $this->assertTrue($on->stopWhenEmpty);
        $this->assertTrue($on->catchThrowable);

        $fallback = WorkerOptions::fromArray(['catch_throwable' => 'nonsense']);
        $this->assertTrue($fallback->catchThrowable, '无法识别的写法回落到默认值');
    }

    #[Test]
    public function fromEnvKeepsLiteralZeroInsteadOfDefault(): void
    {
        $_ENV['QUEUE_RETRY_AFTER'] = '0';

        $config = QueueConfig::fromEnv();

        $this->assertSame(0, $config->connection('memory')->retryAfter);
    }

    #[Test]
    public function reseedAfterForkKeepsIdsValidAndMonotonic(): void
    {
        IdGenerator::ulid();
        IdGenerator::reseedAfterFork();

        $a = IdGenerator::ulid();
        $b = IdGenerator::ulid();

        $this->assertTrue(IdGenerator::isUlid($a));
        $this->assertLessThan(0, strcmp($a, $b), '重新播种后同毫秒仍须单调递增');
        $this->assertEqualsWithDelta(microtime(true) * 1000, (float) IdGenerator::timestampOf($a), 5000.0);
    }
}
