<?php

declare(strict_types=1);

namespace Kode\Queue\Failed;

use Kode\Queue\Contract\FailedJobStoreInterface;
use Kode\Queue\Message\Job;
use Kode\Queue\Support\IdGenerator;
use Override;

/**
 * 进程内失败任务存储。
 *
 * 用于测试与单进程脚本：断言「这个任务确实进了死信」时，
 * 不需要真的起一个数据库。生产环境请换 {@see DatabaseFailedJobStore}。
 */
final class ArrayFailedJobStore implements FailedJobStoreInterface
{
    /** @var array<string, array{id: string, connection: string, queue: string, job: Job, exception: string, failed_at: int}> */
    private array $records = [];

    #[Override]
    public function store(Job $job, \Throwable $exception, string $connection = 'default'): string
    {
        $id = IdGenerator::ulid();

        $this->records[$id] = [
            'id' => $id,
            'connection' => $connection,
            'queue' => $job->queue,
            'job' => $job,
            'exception' => self::format($exception),
            'failed_at' => time(),
        ];

        return $id;
    }

    #[Override]
    public function find(string $id): ?array
    {
        return $this->records[$id] ?? null;
    }

    #[Override]
    public function all(?string $queue = null, int $limit = 100, int $offset = 0): array
    {
        $records = array_values(array_filter(
            $this->records,
            static fn (array $r): bool => $queue === null || $r['queue'] === $queue,
        ));

        // 最新的排在前面。`failed_at` 只有秒精度，同一秒内落下的多条会并列，
        // 因此用 ULID 兜底做二级排序——它在同毫秒内单调自增，字典序即时间序，
        // 这样才能和 FileFailedJobStore（按 ULID 文件名倒序）给出一致的结果。
        usort($records, static fn (array $a, array $b): int => [$b['failed_at'], $b['id']]
            <=> [$a['failed_at'], $a['id']]);

        return \array_slice($records, $offset, $limit);
    }

    #[Override]
    public function count(?string $queue = null): int
    {
        if ($queue === null) {
            return \count($this->records);
        }

        return \count(array_filter($this->records, static fn (array $r): bool => $r['queue'] === $queue));
    }

    #[Override]
    public function forget(string $id): bool
    {
        if (!isset($this->records[$id])) {
            return false;
        }

        unset($this->records[$id]);

        return true;
    }

    #[Override]
    public function flush(?string $queue = null, ?int $olderThan = null): int
    {
        $removed = 0;

        foreach ($this->records as $id => $record) {
            if ($queue !== null && $record['queue'] !== $queue) {
                continue;
            }
            if ($olderThan !== null && $record['failed_at'] > $olderThan) {
                continue;
            }

            unset($this->records[$id]);
            $removed++;
        }

        return $removed;
    }

    /**
     * 异常格式化：类名 + 消息 + 堆栈，一条字符串搞定，方便直接落库。
     */
    public static function format(\Throwable $exception): string
    {
        $lines = [];
        $current = $exception;

        do {
            $lines[] = sprintf(
                '%s: %s in %s:%d',
                $current::class,
                $current->getMessage(),
                $current->getFile(),
                $current->getLine(),
            );
            $current = $current->getPrevious();
        } while ($current !== null);

        return implode("\nCaused by ", $lines) . "\n" . $exception->getTraceAsString();
    }
}
