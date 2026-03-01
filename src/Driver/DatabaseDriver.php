<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use PDO;

class DatabaseDriver implements DriverInterface {
    protected $pdo;
    protected $table;

    public function __construct(array $config = []) {
        $this->table = $config['table'] ?? 'jobs';

        if (isset($config['connection']) && $config['connection'] instanceof PDO) {
            $this->pdo = $config['connection'];
        } else {
            $dsn = $config['dsn'] ?? 'sqlite::memory:';
            $username = $config['username'] ?? null;
            $password = $config['password'] ?? null;

            try {
                $this->pdo = new PDO($dsn, $username, $password);
                $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
                $this->createTable();
            } catch (\PDOException $e) {
                throw new DriverException("Database connection failed: " . $e->getMessage(), 0, $e);
            }
        }
    }

    protected function createTable(): void {
        $sql = "CREATE TABLE IF NOT EXISTS {$this->table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue VARCHAR(255) NOT NULL,
            payload TEXT NOT NULL,
            job_id VARCHAR(255) NOT NULL,
            available_at INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT 0,
            reserved_at INTEGER DEFAULT 0
        )";

        $this->pdo->exec($sql);

        $indexSql = "CREATE INDEX IF NOT EXISTS idx_queue ON {$this->table} (queue)";
        $this->pdo->exec($indexSql);
    }

    public function push(string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $sql = "INSERT INTO {$this->table} (queue, payload, job_id, created_at) VALUES (?, ?, ?, ?)";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$queue, $payload, $jobId, time()]);

        return $jobId;
    }

    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $availableAt = time() + $delay;

        $sql = "INSERT INTO {$this->table} (queue, payload, job_id, available_at, created_at) VALUES (?, ?, ?, ?, ?)";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$queue, $payload, $jobId, $availableAt, time()]);

        return $jobId;
    }

    public function pop(string $queue) {
        $now = time();

        $sql = "SELECT id, payload FROM {$this->table} 
                WHERE queue = ? AND available_at <= ? AND reserved_at = 0 
                ORDER BY id ASC LIMIT 1";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$queue, $now]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        if (!$row) {
            return null;
        }

        $updateSql = "DELETE FROM {$this->table} WHERE id = ?";
        $updateStmt = $this->pdo->prepare($updateSql);
        $updateStmt->execute([$row['id']]);

        return json_decode($row['payload'], true);
    }

    public function size(string $queue): int {
        $now = time();

        $sql = "SELECT COUNT(*) FROM {$this->table} WHERE queue = ? AND available_at <= ?";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$queue, $now]);

        return (int)$stmt->fetchColumn();
    }

    public function delete(string $jobId, string $queue): bool {
        $sql = "DELETE FROM {$this->table} WHERE job_id = ? AND queue = ?";
        $stmt = $this->pdo->prepare($sql);
        return $stmt->execute([$jobId, $queue]);
    }

    public function release(int $delay, string $jobId, string $queue): bool {
        $availableAt = time() + $delay;

        $sql = "UPDATE {$this->table} SET available_at = ?, reserved_at = 0 WHERE job_id = ? AND queue = ?";
        $stmt = $this->pdo->prepare($sql);
        return $stmt->execute([$availableAt, $jobId, $queue]);
    }

    public function stats(string $queue): array {
        $now = time();

        $sql = "SELECT COUNT(*) as total, 
                       SUM(CASE WHEN available_at <= ? THEN 1 ELSE 0 END) as ready,
                       SUM(CASE WHEN available_at > ? THEN 1 ELSE 0 END) as delayed
                FROM {$this->table} WHERE queue = ?";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$now, $now, $queue]);
        $stats = $stmt->fetch(PDO::FETCH_ASSOC);

        return [
            'queue' => $queue,
            'size' => (int)($stats['ready'] ?? 0),
            'total' => (int)($stats['total'] ?? 0),
            'ready' => (int)($stats['ready'] ?? 0),
            'delayed' => (int)($stats['delayed'] ?? 0),
            'timestamp' => time(),
        ];
    }

    public function beginTransaction(): void {
        $this->pdo->beginTransaction();
    }

    public function commit(): void {
        $this->pdo->commit();
    }

    public function rollback(): void {
        $this->pdo->rollBack();
    }

    public function close(): void {
        $this->pdo = null;
    }
}
