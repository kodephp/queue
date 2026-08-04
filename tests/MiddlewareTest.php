<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Exception\QueueException;
use Kode\Queue\Middleware\Operation;
use Kode\Queue\Middleware\Pipeline;
use Kode\Queue\Middleware\RateLimitMiddleware;
use Kode\Queue\Middleware\RetryMiddleware;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(RateLimitMiddleware::class)]
#[CoversClass(RetryMiddleware::class)]
#[CoversClass(Pipeline::class)]
#[CoversClass(Operation::class)]
final class MiddlewareTest extends TestCase
{
    private function operation(string $name = Operation::PUSH): Operation
    {
        return Operation::of($name, 'default', 'default');
    }

    #[Test]
    public function 限流中间件用内置令牌桶拦截超额请求(): void
    {
        $mw = new RateLimitMiddleware(capacity: 2, rate: 100.0, blocking: false);
        $op = $this->operation();
        $called = 0;

        $destination = function (Operation $op) use (&$called): string {
            $called++;

            return 'ok';
        };

        $pipeline = new Pipeline([$mw]);
        $run = static fn () => $pipeline->run($op, $destination);

        self::assertSame('ok', $run());
        self::assertSame('ok', $run());
        self::assertSame(2, $called, '前两个请求应放行');

        $this->expectException(QueueException::class);
        $run(); // 第 3 个：令牌耗尽，非阻塞模式直接抛异常
    }

    #[Test]
    public function 限流中间件支持kode_limiting软依赖桥接(): void
    {
        // 模拟一个 kode/limiting 风格的 Limiter：第一次放，之后全拒
        $limiter = new class {
            private int $calls = 0;

            public function allow(string $key): bool
            {
                return $this->calls++ === 0;
            }

            public function getWaitTime(string $key): float
            {
                return 0.0;
            }
        };

        $mw = new RateLimitMiddleware(capacity: 10, rate: 10.0, blocking: false, limiter: $limiter);
        $op = $this->operation();
        $called = 0;
        $destination = function (Operation $op) use (&$called): string {
            $called++;

            return 'ok';
        };

        $pipeline = new Pipeline([$mw]);

        self::assertSame('ok', $pipeline->run($op, $destination));
        self::assertSame(1, $called);

        $this->expectException(QueueException::class);
        $pipeline->run($op, $destination); // limiter 拒绝 → 抛异常
    }

    #[Test]
    public function 限流中间件未装kode_limiting时回退内置实现(): void
    {
        // withKodeLimiting 在 kode/limiting 不存在时应自动回退，而非抛错
        $mw = RateLimitMiddleware::withKodeLimiting(capacity: 1, rate: 50.0, blocking: false);
        $op = $this->operation();
        $called = 0;
        $destination = function (Operation $op) use (&$called): string {
            $called++;

            return 'ok';
        };
        $pipeline = new Pipeline([$mw]);

        self::assertSame('ok', $pipeline->run($op, $destination));
        self::assertSame(1, $called);

        $this->expectException(QueueException::class);
        $pipeline->run($op, $destination);
    }

    #[Test]
    public function 重试中间件对瞬时失败重试直到成功(): void
    {
        $mw = new RetryMiddleware(maxAttempts: 3, strategy: BackoffStrategy::Fixed, baseDelay: 0);
        $op = $this->operation();
        $attempts = 0;

        $destination = function (Operation $op) use (&$attempts): string {
            $attempts++;
            if ($attempts < 3) {
                throw new RuntimeException('瞬时抖动');
            }

            return 'done';
        };

        $result = (new Pipeline([$mw]))->run($op, $destination);

        self::assertSame('done', $result);
        self::assertSame(3, $attempts, '前两次失败应被重试，第三次成功');
    }

    #[Test]
    public function 重试中间件对持久失败在达到上限后抛出(): void
    {
        $mw = new RetryMiddleware(maxAttempts: 2, strategy: BackoffStrategy::Fixed, baseDelay: 0);
        $op = $this->operation();
        $attempts = 0;

        $destination = function (Operation $op) use (&$attempts): never {
            $attempts++;
            throw new RuntimeException('永久失败');
        };

        $this->expectException(QueueException::class);
        (new Pipeline([$mw]))->run($op, $destination);

        self::assertSame(2, $attempts, '达到上限后应停止重试');
    }

    #[Test]
    public function 重试中间件黑名单中的异常不重试(): void
    {
        $mw = new RetryMiddleware(
            maxAttempts: 5,
            strategy: BackoffStrategy::Fixed,
            baseDelay: 0,
            noRetryOn: [RuntimeException::class],
        );
        $op = $this->operation();
        $attempts = 0;

        $destination = function (Operation $op) use (&$attempts): never {
            $attempts++;
            throw new RuntimeException('黑名单内，不重试');
        };

        $this->expectException(QueueException::class);
        (new Pipeline([$mw]))->run($op, $destination);

        self::assertSame(1, $attempts, '黑名单内的异常应在第一次就冒泡，不再重试');
    }
}
