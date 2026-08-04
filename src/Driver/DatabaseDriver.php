<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Enum\DriverType;
use Kode\Queue\Enum\JobStatus;
use Kode\Queue\Integration\KodeRuntimeBridge;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;
use Override;
use PDO;
use PDOStatement;

/**
 * 关系型数据库驱动（MySQL / PostgreSQL / SQLite）。
 *
 * 数据库队列最容易踩的坑是「两个 Worker 抢到同一行」。v1 用
 * `SELECT ... LIMIT 1` 再 `UPDATE`，中间没有任何互斥，双消费几乎必现。
 *
 * v2 分两条路解决：
 * - MySQL 8.0+ / PostgreSQL 9.5+：`SELECT ... FOR UPDATE SKIP LOCKED`，
 *   被别人锁住的行直接跳过，既不阻塞也不重复；
 * - SQLite 等不支持的后端：退化为乐观抢占——
 *   `UPDATE ... WHERE id = ? AND reserved_at IS NULL`，靠受影响行数判断是否抢到。
 *
 * 另外 v1 的 `delete()` 是空实现，任务处理完仍留在表里；
 * v2 用 `reserved_at` 标记可见性超时，配合 `reclaimExpired()` 实现崩溃恢复。
 */
class DatabaseDriver extends AbstractDriver
{
    private ?PDO $pdo = null;

    /** @var array<string, PDOStatement> 预处理语句缓存 */
    private array $statements = [];

    private ?bool $skipLocked = null;

    private bool $migrated = false;

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Database;
    }

    // ------------------------------------------------------------------
    // 生产
    // ------------------------------------------------------------------

    #[Override]
    public function push(Job $job): string
    {
        $job = $this->normalize($job);

        $this->guard('push', function () use ($job): void {
            $this->statement(
                'insert',
                sprintf(
                    'INSERT INTO %s (job_id, queue, payload, priority, attempts, available_at, created_at, reserved_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, NULL)',
                    $this->table(),
                ),
            )->execute([
                $job->id,
                $job->queue,
                $this->encode($job),
                $job->priority->value,
                $job->attempts,
                $job->availableAt,
                $job->createdAt !== 0 ? $job->createdAt : time(),
            ]);
        }, ['job' => $job->id]);

        return $job->id;
    }

    #[Override]
    public function pushBatch(iterable $jobs): array
    {
        $rows = [];
        $ids = [];

        foreach ($jobs as $job) {
            $job = $this->normalize($job);
            $ids[] = $job->id;
            $rows[] = [
                $job->id,
                $job->queue,
                $this->encode($job),
                $job->priority->value,
                $job->attempts,
                $job->availableAt,
                $job->createdAt !== 0 ? $job->createdAt : time(),
            ];
        }

        if ($rows === []) {
            return [];
        }

        $this->guard('pushBatch', function () use ($rows): void {
            // 单条多值 INSERT：1000 个任务从 1000 次 RTT 降到 1 次
            $placeholders = implode(',', array_fill(0, \count($rows), '(?, ?, ?, ?, ?, ?, ?, NULL)'));
            $sql = sprintf(
                'INSERT INTO %s (job_id, queue, payload, priority, attempts, available_at, created_at, reserved_at) VALUES %s',
                $this->table(),
                $placeholders,
            );

            $this->connection()->prepare($sql)->execute(array_merge(...$rows));
        }, ['count' => \count($rows)]);

        return $ids;
    }

    // ------------------------------------------------------------------
    // 消费
    // ------------------------------------------------------------------

    #[Override]
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob
    {
        $deadline = microtime(true) + max(0.0, $timeout);
        $interval = max(0.005, $this->config->float('poll_interval', 0.2));

        do {
            $this->reclaimExpired($queue);
            $reserved = $this->reserveOne($queue);

            if ($reserved !== null) {
                return $reserved;
            }

            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                break;
            }

            KodeRuntimeBridge::sleep(min($interval, $remaining));
        } while (microtime(true) < $deadline);

        return null;
    }

    #[Override]
    public function peek(string $queue): ?Job
    {
        return $this->guard('peek', function () use ($queue): ?Job {
            $stmt = $this->statement(
                'peek',
                sprintf(
                    'SELECT payload FROM %s
                      WHERE queue = ? AND reserved_at IS NULL AND available_at <= ?
                      ORDER BY priority DESC, id ASC
                      LIMIT 1',
                    $this->table(),
                ),
            );
            $stmt->execute([$queue, time()]);
            $payload = $stmt->fetchColumn();

            return \is_string($payload) ? $this->decode($payload) : null;
        }, ['queue' => $queue]);
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        return $this->guard('ack', function () use ($job): bool {
            $stmt = $this->statement('ack', sprintf('DELETE FROM %s WHERE id = ?', $this->table()));
            $stmt->execute([$job->receipt]);

            return $stmt->rowCount() > 0;
        }, ['job' => $job->job->id]);
    }

    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        return $this->guard('release', function () use ($job, $delay): bool {
            $delay = max(0, $delay);
            $next = $job->job->delayedBy($delay)->withStatus(
                $delay > 0 ? JobStatus::Delayed : JobStatus::Pending,
            );

            $stmt = $this->statement(
                'release',
                sprintf(
                    'UPDATE %s SET reserved_at = NULL, available_at = ?, attempts = ?, payload = ? WHERE id = ?',
                    $this->table(),
                ),
            );
            $stmt->execute([$next->availableAt, $next->attempts, $this->encode($next), $job->receipt]);

            return $stmt->rowCount() > 0;
        }, ['job' => $job->job->id, 'delay' => $delay]);
    }

    #[Override]
    public function fail(ReservedJob $job, \Throwable $reason): bool
    {
        return $this->guard('fail', function () use ($job, $reason): bool {
            $failedTable = $this->config->string('failed_table', 'failed_jobs');

            if ($failedTable !== null && $failedTable !== '') {
                $this->migrate();
                $this->statement(
                    'fail-insert',
                    sprintf(
                        'INSERT INTO %s (job_id, queue, payload, exception, failed_at) VALUES (?, ?, ?, ?, ?)',
                        $this->quote($failedTable),
                    ),
                )->execute([
                    $job->job->id,
                    $job->job->queue,
                    $job->rawPayload !== '' ? $job->rawPayload : $this->encode($job->job),
                    sprintf("%s: %s\n%s", $reason::class, $reason->getMessage(), $reason->getTraceAsString()),
                    time(),
                ]);
            }

            return $this->ack($job);
        }, ['job' => $job->job->id]);
    }

    // ------------------------------------------------------------------
    // 管理
    // ------------------------------------------------------------------

    #[Override]
    public function size(string $queue): int
    {
        return $this->guard('size', function () use ($queue): int {
            $stmt = $this->statement(
                'size',
                sprintf(
                    'SELECT COUNT(*) FROM %s WHERE queue = ? AND reserved_at IS NULL AND available_at <= ?',
                    $this->table(),
                ),
            );
            $stmt->execute([$queue, time()]);

            return (int) $stmt->fetchColumn();
        }, ['queue' => $queue]);
    }

    #[Override]
    public function clear(string $queue): int
    {
        return $this->guard('clear', function () use ($queue): int {
            $stmt = $this->statement('clear', sprintf('DELETE FROM %s WHERE queue = ?', $this->table()));
            $stmt->execute([$queue]);

            return $stmt->rowCount();
        }, ['queue' => $queue]);
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        return $this->guard('stats', function () use ($queue): QueueStats {
            $now = time();
            $stmt = $this->statement(
                'stats',
                sprintf(
                    'SELECT
                        SUM(CASE WHEN reserved_at IS NULL AND available_at <= ? THEN 1 ELSE 0 END) AS ready,
                        SUM(CASE WHEN reserved_at IS NULL AND available_at >  ? THEN 1 ELSE 0 END) AS delayed,
                        SUM(CASE WHEN reserved_at IS NOT NULL THEN 1 ELSE 0 END) AS reserved
                     FROM %s WHERE queue = ?',
                    $this->table(),
                ),
            );
            $stmt->execute([$now, $now, $queue]);
            /** @var array{ready: ?int, delayed: ?int, reserved: ?int}|false $row */
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            return QueueStats::of(
                queue: $queue,
                driver: $this->type()->value,
                ready: (int) ($row['ready'] ?? 0),
                delayed: (int) ($row['delayed'] ?? 0),
                reserved: (int) ($row['reserved'] ?? 0),
                failed: $this->failedCount($queue),
                extra: ['table' => $this->config->string('table', 'jobs')],
            );
        }, ['queue' => $queue]);
    }

    #[Override]
    public function reclaimExpired(string $queue): int
    {
        return $this->guard('reclaimExpired', function () use ($queue): int {
            $stmt = $this->statement(
                'reclaim',
                sprintf(
                    'UPDATE %s SET reserved_at = NULL WHERE queue = ? AND reserved_at IS NOT NULL AND reserved_at <= ?',
                    $this->table(),
                ),
            );
            $stmt->execute([$queue, time() - $this->retryAfter()]);

            return $stmt->rowCount();
        }, ['queue' => $queue]);
    }

    // ------------------------------------------------------------------
    // 事务
    // ------------------------------------------------------------------

    #[Override]
    protected function beginTransaction(): void
    {
        $this->connection()->beginTransaction();
    }

    #[Override]
    protected function commitTransaction(): void
    {
        $this->connection()->commit();
    }

    #[Override]
    protected function rollbackTransaction(): void
    {
        if ($this->connection()->inTransaction()) {
            $this->connection()->rollBack();
        }
    }

    // ------------------------------------------------------------------
    // 连接
    // ------------------------------------------------------------------

    #[Override]
    public function isConnected(): bool
    {
        if ($this->pdo === null) {
            return false;
        }

        try {
            $this->pdo->query('SELECT 1');

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    #[Override]
    public function reconnect(): void
    {
        $this->close();
        $this->connection();
    }

    #[Override]
    public function close(): void
    {
        $this->statements = [];
        $this->pdo = null;
        $this->connected = false;
        $this->migrated = false;
        $this->skipLocked = null;
    }

    /**
     * 底层 PDO 连接（惰性建立，可注入现成实例复用框架连接池）。
     */
    public function connection(): PDO
    {
        if ($this->pdo instanceof PDO) {
            return $this->pdo;
        }

        $injected = $this->config->object('pdo') ?? $this->config->object('connection');

        if ($injected instanceof PDO) {
            $this->pdo = $injected;
        } else {
            $this->pdo = new PDO(
                $this->config->requireString('dsn'),
                $this->config->string('username'),
                $this->config->secret('password'),
                [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_EMULATE_PREPARES => false,
                    ...$this->config->array('pdo_options'),
                ],
            );
        }

        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->connected = true;
        $this->migrate();

        return $this->pdo;
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private function reserveOne(string $queue): ?ReservedJob
    {
        return $this->guard('pop', function () use ($queue): ?ReservedJob {
            return $this->supportsSkipLocked()
                ? $this->reserveWithSkipLocked($queue)
                : $this->reserveOptimistically($queue);
        }, ['queue' => $queue]);
    }

    /**
     * MySQL 8 / PostgreSQL 9.5+：行级锁 + SKIP LOCKED，一次事务内锁定并标记。
     */
    private function reserveWithSkipLocked(string $queue): ?ReservedJob
    {
        $pdo = $this->connection();
        $nested = $pdo->inTransaction();

        if (!$nested) {
            $pdo->beginTransaction();
        }

        try {
            $stmt = $pdo->prepare(sprintf(
                'SELECT id, payload, attempts FROM %s
                  WHERE queue = ? AND reserved_at IS NULL AND available_at <= ?
                  ORDER BY priority DESC, id ASC
                  LIMIT 1 FOR UPDATE SKIP LOCKED',
                $this->table(),
            ));
            $stmt->execute([$queue, time()]);
            /** @var array{id: int|string, payload: string, attempts: int}|false $row */
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            if ($row === false) {
                if (!$nested) {
                    $pdo->commit();
                }

                return null;
            }

            $reserved = $this->markReserved($pdo, $row);

            if (!$nested) {
                $pdo->commit();
            }

            return $reserved;
        } catch (\Throwable $e) {
            if (!$nested && $pdo->inTransaction()) {
                $pdo->rollBack();
            }

            throw $e;
        }
    }

    /**
     * SQLite 等无 SKIP LOCKED 的后端：靠 UPDATE 的受影响行数做乐观抢占。
     */
    private function reserveOptimistically(string $queue): ?ReservedJob
    {
        $pdo = $this->connection();

        $select = $this->statement(
            'reserve-select',
            sprintf(
                'SELECT id, payload, attempts FROM %s
                  WHERE queue = ? AND reserved_at IS NULL AND available_at <= ?
                  ORDER BY priority DESC, id ASC
                  LIMIT 1',
                $this->table(),
            ),
        );

        // 最多重试若干次：每次失败都意味着有别的 Worker 抢先了
        for ($attempt = 0; $attempt < 8; $attempt++) {
            $select->execute([$queue, time()]);
            /** @var array{id: int|string, payload: string, attempts: int}|false $row */
            $row = $select->fetch(PDO::FETCH_ASSOC);

            if ($row === false) {
                return null;
            }

            $claim = $this->statement(
                'reserve-claim',
                sprintf(
                    'UPDATE %s SET reserved_at = ?, attempts = attempts + 1 WHERE id = ? AND reserved_at IS NULL',
                    $this->table(),
                ),
            );
            $claim->execute([time(), $row['id']]);

            if ($claim->rowCount() > 0) {
                $job = $this->decode($row['payload'])->incrementAttempts();

                return ReservedJob::of($job, $row['id'], $row['payload'], ['row_id' => $row['id']]);
            }
        }

        return null;
    }

    /**
     * @param array{id: int|string, payload: string, attempts: int} $row
     */
    private function markReserved(PDO $pdo, array $row): ReservedJob
    {
        $stmt = $pdo->prepare(sprintf(
            'UPDATE %s SET reserved_at = ?, attempts = attempts + 1 WHERE id = ?',
            $this->table(),
        ));
        $stmt->execute([time(), $row['id']]);

        $job = $this->decode($row['payload'])->incrementAttempts();

        return ReservedJob::of($job, $row['id'], $row['payload'], ['row_id' => $row['id']]);
    }

    private function failedCount(string $queue): int
    {
        $failedTable = $this->config->string('failed_table', 'failed_jobs');

        if ($failedTable === null || $failedTable === '') {
            return -1;
        }

        try {
            $stmt = $this->connection()->prepare(sprintf(
                'SELECT COUNT(*) FROM %s WHERE queue = ?',
                $this->quote($failedTable),
            ));
            $stmt->execute([$queue]);

            return (int) $stmt->fetchColumn();
        } catch (\Throwable) {
            return -1;
        }
    }

    /**
     * 预处理语句缓存：长驻 Worker 里同一条 SQL 只解析一次。
     */
    private function statement(string $key, string $sql): PDOStatement
    {
        return $this->statements[$key] ??= $this->connection()->prepare($sql);
    }

    private function driverName(): string
    {
        return strtolower((string) $this->connection()->getAttribute(PDO::ATTR_DRIVER_NAME));
    }

    private function supportsSkipLocked(): bool
    {
        if ($this->skipLocked !== null) {
            return $this->skipLocked;
        }

        if ($this->config->has('skip_locked')) {
            return $this->skipLocked = $this->config->bool('skip_locked');
        }

        $version = (string) $this->connection()->getAttribute(PDO::ATTR_SERVER_VERSION);

        return $this->skipLocked = match ($this->driverName()) {
            'mysql' => version_compare($version, '8.0.1', '>=')
                && stripos($version, 'mariadb') === false,
            'pgsql' => version_compare($version, '9.5', '>='),
            default => false,
        };
    }

    private function table(): string
    {
        return $this->quote($this->config->string('table', 'jobs') ?? 'jobs');
    }

    /**
     * 标识符引用：只允许字母数字下划线，从根上杜绝表名注入。
     */
    private function quote(string $identifier): string
    {
        $safe = preg_replace('/[^A-Za-z0-9_]/', '', $identifier) ?? '';

        return match ($this->driverName()) {
            'mysql' => '`' . $safe . '`',
            default => '"' . $safe . '"',
        };
    }

    /**
     * 自动建表。生产环境可关闭（`auto_migrate` = false）改用正式迁移脚本。
     */
    private function migrate(): void
    {
        if ($this->migrated || !$this->config->bool('auto_migrate', true)) {
            $this->migrated = true;

            return;
        }

        $this->migrated = true;
        $pdo = $this->pdo;

        if (!$pdo instanceof PDO) {
            return;
        }

        $autoIncrement = match ($this->driverName()) {
            'mysql' => 'BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY',
            'pgsql' => 'BIGSERIAL PRIMARY KEY',
            default => 'INTEGER PRIMARY KEY AUTOINCREMENT',
        };

        $jobs = $this->table();
        $pdo->exec(sprintf(
            'CREATE TABLE IF NOT EXISTS %s (
                id %s,
                job_id VARCHAR(64) NOT NULL,
                queue VARCHAR(128) NOT NULL,
                payload TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 50,
                attempts INTEGER NOT NULL DEFAULT 0,
                reserved_at INTEGER NULL,
                available_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )',
            $jobs,
            $autoIncrement,
        ));

        // 索引名带表名前缀，避免多张队列表冲突
        $suffix = preg_replace('/[^A-Za-z0-9_]/', '', $this->config->string('table', 'jobs') ?? 'jobs');
        foreach ([
            "kq_{$suffix}_pop" => '(queue, reserved_at, available_at, priority)',
            "kq_{$suffix}_job" => '(job_id)',
        ] as $name => $columns) {
            try {
                $pdo->exec(sprintf('CREATE INDEX IF NOT EXISTS %s ON %s %s', $name, $jobs, $columns));
            } catch (\Throwable) {
                // MySQL 5.x 不支持 IF NOT EXISTS，重复创建会报错，忽略即可
            }
        }

        $failedTable = $this->config->string('failed_table', 'failed_jobs');
        if ($failedTable !== null && $failedTable !== '') {
            $pdo->exec(sprintf(
                'CREATE TABLE IF NOT EXISTS %s (
                    id %s,
                    job_id VARCHAR(64) NOT NULL,
                    queue VARCHAR(128) NOT NULL,
                    payload TEXT NOT NULL,
                    exception TEXT NOT NULL,
                    failed_at INTEGER NOT NULL
                )',
                $this->quote($failedTable),
                $autoIncrement,
            ));
        }
    }
}
