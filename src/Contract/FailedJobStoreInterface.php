<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

use Kode\Queue\Message\Job;

/**
 * 失败任务存储契约。
 *
 * v1 完全没有失败任务的概念——重试耗尽后任务就消失了。
 * v2 强制要求 Worker 把死信落库，可查询、可重放、可清理。
 */
interface FailedJobStoreInterface
{
    /**
     * 记录一条失败任务，返回存储 ID。
     */
    public function store(Job $job, \Throwable $exception, string $connection = 'default'): string;

    /**
     * 按 ID 取回。
     *
     * @return array{id: string, connection: string, queue: string, job: Job, exception: string, failed_at: int}|null
     */
    public function find(string $id): ?array;

    /**
     * 列出失败任务。
     *
     * @return list<array{id: string, connection: string, queue: string, job: Job, exception: string, failed_at: int}>
     */
    public function all(?string $queue = null, int $limit = 100, int $offset = 0): array;

    /**
     * 数量统计。
     */
    public function count(?string $queue = null): int;

    /**
     * 删除一条。
     */
    public function forget(string $id): bool;

    /**
     * 清空（可按队列），返回删除条数。
     */
    public function flush(?string $queue = null, ?int $olderThan = null): int;
}
