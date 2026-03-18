<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use PDO;

/**
 * 数据库队列驱动
 * 
 * 使用关系型数据库（MySQL、PostgreSQL、SQLite等）作为队列存储后端。
 * 支持事务、延迟任务和持久化存储。
 */
class DatabaseDriver implements DriverInterface {
    /**
     * PDO 数据库连接实例
     *
     * @var PDO
     */
    protected PDO $pdo;

    /**
     * 任务表名称
     *
     * @var string
     */
    protected string $table;

    /**
     * 创建数据库驱动实例
     *
     * @param array $config 配置数组
     *                       - dsn: PDO DSN 连接字符串
     *                       - username: 数据库用户名
     *                       - password: 数据库密码
     *                       - table: 任务表名称
     *                       - connection: PDO 实例（可选，直接传入已建立的连接）
     * @throws DriverException 数据库连接失败时抛出
     */
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

    /**
     * 创建任务表
     * 
     * 如果表不存在则自动创建，包含必要的索引
     *
     * @return void
     */
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

    /**
     * 推送任务到队列
     *
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称
     * @param array  $options 额外选项
     * @return string 任务ID
     */
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

    /**
     * 延迟推送任务到队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称
     * @param array  $options 额外选项
     * @return string 任务ID
     */
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

    /**
     * 从队列中取出下一个任务
     * 
     * 只返回已到期且未被预留的任务
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据数组，无任务时返回null
     */
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

        // 删除已取出的任务
        $updateSql = "DELETE FROM {$this->table} WHERE id = ?";
        $updateStmt = $this->pdo->prepare($updateSql);
        $updateStmt->execute([$row['id']]);

        return json_decode($row['payload'], true);
    }

    /**
     * 获取队列大小
     * 
     * 只统计已到期的任务数量
     *
     * @param string $queue 队列名称
     * @return int 队列中的任务数量
     */
    public function size(string $queue): int {
        $now = time();

        $sql = "SELECT COUNT(*) FROM {$this->table} WHERE queue = ? AND available_at <= ?";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$queue, $now]);

        return (int)$stmt->fetchColumn();
    }

    /**
     * 从队列中删除任务
     *
     * @param string $jobId 任务ID
     * @param string $queue 队列名称
     * @return bool 是否删除成功
     */
    public function delete(string $jobId, string $queue): bool {
        $sql = "DELETE FROM {$this->table} WHERE job_id = ? AND queue = ?";
        $stmt = $this->pdo->prepare($sql);
        return $stmt->execute([$jobId, $queue]);
    }

    /**
     * 将任务释放回队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $jobId 任务ID
     * @param string $queue 队列名称
     * @return bool 是否释放成功
     */
    public function release(int $delay, string $jobId, string $queue): bool {
        $availableAt = time() + $delay;

        $sql = "UPDATE {$this->table} SET available_at = ?, reserved_at = 0 WHERE job_id = ? AND queue = ?";
        $stmt = $this->pdo->prepare($sql);
        return $stmt->execute([$availableAt, $jobId, $queue]);
    }

    /**
     * 获取队列统计信息
     *
     * @param string $queue 队列名称
     * @return array 统计信息数组，包含 total、ready、delayed 等字段
     */
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

    /**
     * 开始事务
     *
     * @return void
     */
    public function beginTransaction(): void {
        $this->pdo->beginTransaction();
    }

    /**
     * 提交事务
     *
     * @return void
     */
    public function commit(): void {
        $this->pdo->commit();
    }

    /**
     * 回滚事务
     *
     * @return void
     */
    public function rollback(): void {
        $this->pdo->rollBack();
    }

    /**
     * 关闭数据库连接
     *
     * @return void
     */
    public function close(): void {
        $this->pdo = null;
    }
}
