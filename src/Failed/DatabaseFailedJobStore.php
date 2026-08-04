<?php

declare(strict_types=1);

namespace Kode\Queue\Failed;

use Kode\Queue\Contract\FailedJobStoreInterface;
use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Message\Job;
use Kode\Queue\Serializer\SerializerFactory;
use Kode\Queue\Support\IdGenerator;
use Override;
use PDO;

/**
 * 数据库失败任务存储（生产推荐）。
 *
 * 死信必须落在能被 SQL 查询的地方——运维要能回答
 * 「昨天 mail 队列失败了多少条、都是什么异常、能不能一键重放」。
 * 存 Redis 列表做不到这些，所以这里独立于队列驱动，专用一张表。
 *
 * 表结构与 {@see \Kode\Queue\Driver\DatabaseDriver} 自动创建的 `failed_jobs`
 * 完全兼容，二者可以共用同一张表。
 */
final class DatabaseFailedJobStore implements FailedJobStoreInterface
{
    private bool $migrated = false;

    private readonly SerializerInterface $serializer;

    public function __construct(
        private readonly PDO $pdo,
        private readonly string $table = 'failed_jobs',
        ?SerializerInterface $serializer = null,
        private readonly bool $autoMigrate = true,
    ) {
        $this->serializer = $serializer ?? SerializerFactory::make('json');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    /**
     * 用 DSN 直接构造（脱离 QueueManager 单独使用时方便）。
     */
    public static function fromDsn(
        string $dsn,
        ?string $username = null,
        ?string $password = null,
        string $table = 'failed_jobs',
    ): self {
        return new self(new PDO($dsn, $username, $password, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]), $table);
    }

    #[Override]
    public function store(Job $job, \Throwable $exception, string $connection = 'default'): string
    {
        $this->migrate();
        $id = IdGenerator::ulid();

        $stmt = $this->pdo->prepare(sprintf(
            'INSERT INTO %s (uuid, job_id, connection, queue, payload, exception, failed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)',
            $this->quoted(),
        ));
        $stmt->execute([
            $id,
            $job->id,
            $connection,
            $job->queue,
            $this->serializer->serialize($job),
            ArrayFailedJobStore::format($exception),
            time(),
        ]);

        return $id;
    }

    #[Override]
    public function find(string $id): ?array
    {
        $this->migrate();

        $stmt = $this->pdo->prepare(sprintf('SELECT * FROM %s WHERE uuid = ?', $this->quoted()));
        $stmt->execute([$id]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        return \is_array($row) ? $this->hydrate($row) : null;
    }

    #[Override]
    public function all(?string $queue = null, int $limit = 100, int $offset = 0): array
    {
        $this->migrate();

        $sql = sprintf('SELECT * FROM %s', $this->quoted());
        $bindings = [];

        if ($queue !== null) {
            $sql .= ' WHERE queue = ?';
            $bindings[] = $queue;
        }

        $sql .= ' ORDER BY failed_at DESC, id DESC LIMIT ? OFFSET ?';
        $bindings[] = max(1, $limit);
        $bindings[] = max(0, $offset);

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($bindings);

        return array_map($this->hydrate(...), $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    #[Override]
    public function count(?string $queue = null): int
    {
        $this->migrate();

        $sql = sprintf('SELECT COUNT(*) FROM %s', $this->quoted());
        $bindings = [];

        if ($queue !== null) {
            $sql .= ' WHERE queue = ?';
            $bindings[] = $queue;
        }

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($bindings);

        return (int) $stmt->fetchColumn();
    }

    #[Override]
    public function forget(string $id): bool
    {
        $this->migrate();

        $stmt = $this->pdo->prepare(sprintf('DELETE FROM %s WHERE uuid = ?', $this->quoted()));
        $stmt->execute([$id]);

        return $stmt->rowCount() > 0;
    }

    #[Override]
    public function flush(?string $queue = null, ?int $olderThan = null): int
    {
        $this->migrate();

        $sql = sprintf('DELETE FROM %s', $this->quoted());
        $conditions = [];
        $bindings = [];

        if ($queue !== null) {
            $conditions[] = 'queue = ?';
            $bindings[] = $queue;
        }

        if ($olderThan !== null) {
            $conditions[] = 'failed_at <= ?';
            $bindings[] = $olderThan;
        }

        if ($conditions !== []) {
            $sql .= ' WHERE ' . implode(' AND ', $conditions);
        }

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($bindings);

        return $stmt->rowCount();
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /**
     * @param  array<string, mixed> $row
     * @return array{id: string, connection: string, queue: string, job: Job, exception: string, failed_at: int}
     */
    private function hydrate(array $row): array
    {
        return [
            'id' => (string) ($row['uuid'] ?? $row['id'] ?? ''),
            'connection' => (string) ($row['connection'] ?? 'default'),
            'queue' => (string) ($row['queue'] ?? 'default'),
            'job' => $this->serializer->deserialize((string) ($row['payload'] ?? '{}')),
            'exception' => (string) ($row['exception'] ?? ''),
            'failed_at' => (int) ($row['failed_at'] ?? 0),
        ];
    }

    private function driverName(): string
    {
        return strtolower((string) $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME));
    }

    private function quoted(): string
    {
        $safe = preg_replace('/[^A-Za-z0-9_]/', '', $this->table) ?? 'failed_jobs';

        return $this->driverName() === 'mysql' ? "`{$safe}`" : "\"{$safe}\"";
    }

    private function migrate(): void
    {
        if ($this->migrated) {
            return;
        }

        $this->migrated = true;

        if (!$this->autoMigrate) {
            return;
        }

        $autoIncrement = match ($this->driverName()) {
            'mysql' => 'BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY',
            'pgsql' => 'BIGSERIAL PRIMARY KEY',
            default => 'INTEGER PRIMARY KEY AUTOINCREMENT',
        };

        $this->pdo->exec(sprintf(
            'CREATE TABLE IF NOT EXISTS %s (
                id %s,
                uuid VARCHAR(64) NOT NULL,
                job_id VARCHAR(64) NOT NULL,
                connection VARCHAR(64) NOT NULL DEFAULT \'default\',
                queue VARCHAR(128) NOT NULL,
                payload TEXT NOT NULL,
                exception TEXT NOT NULL,
                failed_at INTEGER NOT NULL
            )',
            $this->quoted(),
            $autoIncrement,
        ));

        try {
            $this->pdo->exec(sprintf(
                'CREATE UNIQUE INDEX IF NOT EXISTS kq_failed_uuid ON %s (uuid)',
                $this->quoted(),
            ));
        } catch (\Throwable) {
            // MySQL 5.x 不支持 IF NOT EXISTS，重复创建报错可忽略
        }
    }
}
