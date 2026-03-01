<?php

namespace Kode\Queue\Driver;

use PDO;

class DatabaseDriver implements DriverInterface {
    /**
     * PDO 连接
     *
     * @var PDO
     */
    protected $pdo;

    /**
     * 表名
     *
     * @var string
     */
    protected $table;

    /**
     * 创建新的数据库驱动实例
     *
     * @param array $config 配置
     */
    public function __construct(array $config = []) {
        $this->table = $config['table'] ?? 'jobs';
        $this->pdo = $this->createConnection($config);
        $this->createTable();
    }

    /**
     * 创建数据库连接
     *
     * @param array $config 配置
     * @return PDO
     */
    protected function createConnection(array $config): PDO {
        if (isset($config['pdo'])) {
            return $config['pdo'];
        }

        $dsn = $config['dsn'] ?? 'mysql:host=127.0.0.1;dbname=test';
        $username = $config['username'] ?? 'root';
        $password = $config['password'] ?? '';

        return new PDO($dsn, $username, $password, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);
    }

    /**
     * 创建任务表
     *
     * @return void
     */
    protected function createTable(): void {
        $sql = "
            CREATE TABLE IF NOT EXISTS {$this->table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                queue VARCHAR(255) NOT NULL,
                payload TEXT NOT NULL,
                attempts INT DEFAULT 0,
                reserved_at TIMESTAMP NULL,
                available_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ";

        $this->pdo->exec($sql);
    }

    /**
     * 推送任务到队列
     *
     * @param string $payload 任务负载
     * @param string $queue 队列名称
     * @param array  $options 选项
     * @return string 任务ID
     */
    public function push(string $payload, string $queue, array $options = []): string {
        $availableAt = date('Y-m-d H:i:s');
        $createdAt = date('Y-m-d H:i:s');

        $stmt = $this->pdo->prepare(
            "INSERT INTO {$this->table} (queue, payload, available_at, created_at) VALUES (?, ?, ?, ?)"
        );

        $stmt->execute([$queue, $payload, $availableAt, $createdAt]);

        return (string) $this->pdo->lastInsertId();
    }

    /**
     * 延迟推送任务到队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $payload 任务负载
     * @param string $queue 队列名称
     * @param array  $options 选项
     * @return string 任务ID
     */
    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $availableAt = date('Y-m-d H:i:s', time() + $delay);
        $createdAt = date('Y-m-d H:i:s');

        $stmt = $this->pdo->prepare(
            "INSERT INTO {$this->table} (queue, payload, available_at, created_at) VALUES (?, ?, ?, ?)"
        );

        $stmt->execute([$queue, $payload, $availableAt, $createdAt]);

        return (string) $this->pdo->lastInsertId();
    }

    /**
     * 从队列中取出下一个任务
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据
     */
    public function pop(string $queue) {
        $now = date('Y-m-d H:i:s');

        // 使用事务确保原子性
        $this->pdo->beginTransaction();

        try {
            // 锁定并获取任务
            $stmt = $this->pdo->prepare(
                "SELECT * FROM {$this->table} WHERE queue = ? AND available_at <= ? AND reserved_at IS NULL ORDER BY available_at ASC LIMIT 1 FOR UPDATE"
            );

            $stmt->execute([$queue, $now]);
            $job = $stmt->fetch();

            if (!$job) {
                $this->pdo->commit();
                return null;
            }

            // 更新任务状态为已保留
            $reservedAt = date('Y-m-d H:i:s');
            $stmt = $this->pdo->prepare(
                "UPDATE {$this->table} SET reserved_at = ?, attempts = attempts + 1 WHERE id = ?"
            );

            $stmt->execute([$reservedAt, $job['id']]);

            $this->pdo->commit();

            $payload = json_decode($job['payload'], true);
            $payload['id'] = (string) $job['id'];

            return $payload;
        } catch (\Exception $e) {
            $this->pdo->rollback();
            throw $e;
        }
    }

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称
     * @return int 队列大小
     */
    public function size(string $queue): int {
        $stmt = $this->pdo->prepare(
            "SELECT COUNT(*) FROM {$this->table} WHERE queue = ? AND available_at <= ? AND reserved_at IS NULL"
        );

        $stmt->execute([$queue, date('Y-m-d H:i:s')]);

        return (int) $stmt->fetchColumn();
    }

    /**
     * 从队列中删除任务
     *
     * @param string $jobId 任务ID
     * @param string $queue 队列名称
     * @return bool 是否删除成功
     */
    public function delete(string $jobId, string $queue): bool {
        $stmt = $this->pdo->prepare(
            "DELETE FROM {$this->table} WHERE id = ? AND queue = ?"
        );

        $stmt->execute([$jobId, $queue]);

        return $stmt->rowCount() > 0;
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
        $availableAt = date('Y-m-d H:i:s', time() + $delay);

        $stmt = $this->pdo->prepare(
            "UPDATE {$this->table} SET reserved_at = NULL, available_at = ? WHERE id = ? AND queue = ?"
        );

        $stmt->execute([$availableAt, $jobId, $queue]);

        return $stmt->rowCount() > 0;
    }

    /**
     * 获取队列统计信息
     *
     * @param string $queue 队列名称
     * @return array 统计信息
     */
    public function stats(string $queue): array {
        $stmt = $this->pdo->prepare(
            "SELECT COUNT(*) as total, SUM(CASE WHEN reserved_at IS NOT NULL THEN 1 ELSE 0 END) as reserved FROM {$this->table} WHERE queue = ?"
        );

        $stmt->execute([$queue]);
        $stats = $stmt->fetch();

        return [
            'queue' => $queue,
            'size' => $this->size($queue),
            'total_jobs' => (int) $stats['total'],
            'reserved_jobs' => (int) $stats['reserved'],
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
        $this->pdo->rollback();
    }

    /**
     * 关闭连接
     *
     * @return void
     */
    public function close(): void {
        // PDO 连接会在对象销毁时自动关闭
    }
}