<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use Kode\Queue\Contract\FailedJobStoreInterface;
use Kode\Queue\Failed\ArrayFailedJobStore;
use Kode\Queue\Failed\FileFailedJobStore;
use Kode\Queue\Message\Job;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(ArrayFailedJobStore::class)]
#[CoversClass(FileFailedJobStore::class)]
final class FailedJobStoreTest extends TestCase
{
    private string $tempDir = '';

    protected function tearDown(): void
    {
        if ($this->tempDir !== '' && is_dir($this->tempDir)) {
            $this->removeDirectory($this->tempDir);
        }
    }

    /**
     * @return iterable<string, array{callable(self): FailedJobStoreInterface}>
     */
    public static function stores(): iterable
    {
        yield '内存存储' => [static fn (self $case): FailedJobStoreInterface => new ArrayFailedJobStore()];
        yield '文件存储' => [static fn (self $case): FailedJobStoreInterface => $case->fileStore()];
    }

    #[Test]
    #[DataProvider('stores')]
    public function 落库后可按_id_取回(callable $factory): void
    {
        $store = $factory($this);
        $job = Job::create('demo.boom', ['x' => 1], 'default');

        $id = $store->store($job, new RuntimeException('炸了'), 'redis');
        $record = $store->find($id);

        self::assertNotNull($record);
        self::assertSame($id, $record['id']);
        self::assertSame('redis', $record['connection']);
        self::assertSame('default', $record['queue']);
        self::assertSame($job->id, $record['job']->id);
        self::assertSame(['x' => 1], $record['job']->payload);
        self::assertStringContainsString('炸了', $record['exception']);
        self::assertGreaterThan(0, $record['failed_at']);
    }

    #[Test]
    #[DataProvider('stores')]
    public function 按队列统计与筛选(callable $factory): void
    {
        $store = $factory($this);

        $store->store(Job::create('a', queue: 'default'), new RuntimeException('1'));
        $store->store(Job::create('b', queue: 'mails'), new RuntimeException('2'));
        $store->store(Job::create('c', queue: 'mails'), new RuntimeException('3'));

        self::assertSame(3, $store->count());
        self::assertSame(2, $store->count('mails'));
        self::assertCount(2, $store->all('mails'));
        self::assertCount(1, $store->all('default'));
    }

    #[Test]
    #[DataProvider('stores')]
    public function 列表按失败时间倒序(callable $factory): void
    {
        $store = $factory($this);

        $store->store(Job::create('first'), new RuntimeException('1'));
        $store->store(Job::create('second'), new RuntimeException('2'));
        $store->store(Job::create('third'), new RuntimeException('3'));

        $names = array_map(static fn (array $r): string => $r['job']->name, $store->all());

        self::assertSame(['third', 'second', 'first'], $names);
    }

    #[Test]
    #[DataProvider('stores')]
    public function 分页参数生效(callable $factory): void
    {
        $store = $factory($this);

        foreach (range(1, 5) as $n) {
            $store->store(Job::create('job.' . $n), new RuntimeException((string) $n));
        }

        self::assertCount(2, $store->all(null, 2));
        self::assertCount(3, $store->all(null, 10, 2));
        self::assertSame([], $store->all(null, 10, 99));
    }

    #[Test]
    #[DataProvider('stores')]
    public function forget_只删指定的那条(callable $factory): void
    {
        $store = $factory($this);

        $keep = $store->store(Job::create('keep'), new RuntimeException('1'));
        $drop = $store->store(Job::create('drop'), new RuntimeException('2'));

        self::assertTrue($store->forget($drop));
        self::assertFalse($store->forget($drop), '重复删除应返回 false');
        self::assertNull($store->find($drop));
        self::assertNotNull($store->find($keep));
        self::assertSame(1, $store->count());
    }

    #[Test]
    #[DataProvider('stores')]
    public function flush_可按队列清空(callable $factory): void
    {
        $store = $factory($this);

        $store->store(Job::create('a', queue: 'default'), new RuntimeException('1'));
        $store->store(Job::create('b', queue: 'mails'), new RuntimeException('2'));

        self::assertSame(1, $store->flush('mails'));
        self::assertSame(0, $store->count('mails'));
        self::assertSame(1, $store->count());

        self::assertSame(1, $store->flush());
        self::assertSame(0, $store->count());
    }

    #[Test]
    #[DataProvider('stores')]
    public function 未知_id_返回_null(callable $factory): void
    {
        $store = $factory($this);

        self::assertNull($store->find('01J0000000000000000000000X'));
        self::assertNull($store->find('../../etc/passwd'), '非法 ID 不得参与路径拼接');
    }

    #[Test]
    public function 异常链会被完整格式化(): void
    {
        $store = new ArrayFailedJobStore();
        $inner = new RuntimeException('底层连接断开');
        $outer = new RuntimeException('任务执行失败', 0, $inner);

        $record = $store->find($store->store(Job::create('x'), $outer));

        self::assertNotNull($record);
        self::assertStringContainsString('任务执行失败', $record['exception']);
        self::assertStringContainsString('Caused by', $record['exception']);
        self::assertStringContainsString('底层连接断开', $record['exception']);
    }

    #[Test]
    public function 文件存储按队列分目录且可统计磁盘占用(): void
    {
        $store = $this->fileStore();

        $store->store(Job::create('a', queue: 'default'), new RuntimeException('1'));
        $store->store(Job::create('b', queue: '订单/高优先级'), new RuntimeException('2'));

        $buckets = glob($this->tempDir . '/*', \GLOB_ONLYDIR) ?: [];

        self::assertCount(2, $buckets, '不同队列应落在不同目录');
        self::assertGreaterThan(0, $store->diskUsage());
        self::assertSame(1, $store->count('订单/高优先级'), '含特殊字符的队列名也要能查回来');
    }

    #[Test]
    public function 文件存储不会留下临时文件(): void
    {
        $store = $this->fileStore();
        $store->store(Job::create('a'), new RuntimeException('1'));

        $temps = glob($this->tempDir . '/*/*.tmp') ?: [];

        self::assertSame([], $temps);
    }

    public function fileStore(): FileFailedJobStore
    {
        if ($this->tempDir === '') {
            $this->tempDir = sys_get_temp_dir() . '/kode-queue-failed-' . bin2hex(random_bytes(6));
        }

        return new FileFailedJobStore($this->tempDir);
    }

    private function removeDirectory(string $path): void
    {
        $items = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($path, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );

        foreach ($items as $item) {
            $item->isDir() ? @rmdir($item->getPathname()) : @unlink($item->getPathname());
        }

        @rmdir($path);
    }
}
