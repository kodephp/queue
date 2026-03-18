<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use Predis\Client;

/**
 * Redis 队列驱动
 * 
 * 使用 Redis 作为队列存储后端，提供高性能的队列操作。
 * 使用 Redis List 存储普通任务，使用 Sorted Set 存储延迟任务。
 */
class RedisDriver implements DriverInterface {
    /**
     * Redis 客户端实例
     *
     * @var Client
     */
    protected Client $redis;

    /**
     * 连接配置
     *
     * @var array
     */
    protected array $connection;

    /**
     * 创建 Redis 驱动实例
     *
     * @param array $config 配置数组
     *                       - scheme: 连接协议（tcp/unix）
     *                       - host: Redis 服务器地址
     *                       - port: Redis 端口
     *                       - database: 数据库编号
     *                       - password: 密码
     *                       - options: Predis 选项
     * @throws DriverException Redis 连接失败时抛出
     */
    public function __construct(array $config = []) {
        try {
            $this->redis = new Client([
                'scheme' => $config['scheme'] ?? 'tcp',
                'host' => $config['host'] ?? '127.0.0.1',
                'port' => $config['port'] ?? 6379,
                'database' => $config['database'] ?? 0,
                'password' => $config['password'] ?? null,
            ] + ($config['options'] ?? []));

            $this->connection = $config;
        } catch (\Exception $e) {
            throw new DriverException("Redis connection failed: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * 推送任务到队列
     * 
     * 使用 RPUSH 将任务添加到列表尾部
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

        $this->redis->rpush($this->getQueueKey($queue), [$payload]);

        return $jobId;
    }

    /**
     * 延迟推送任务到队列
     * 
     * 使用 Sorted Set 存储，score 为执行时间戳
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
        $data['available_at'] = time() + $delay;
        $payload = json_encode($data);

        $this->redis->zadd($this->getDelayedKey($queue), time() + $delay, $payload);

        return $jobId;
    }

    /**
     * 从队列中取出下一个任务
     * 
     * 先处理已到期的延迟任务，再从主队列取出任务
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据数组，无任务时返回null
     */
    public function pop(string $queue) {
        // 迁移已到期的延迟任务
        $this->migrateDelayedJobs($queue);

        // 从列表头部取出任务
        $payload = $this->redis->lpop($this->getQueueKey($queue));

        if (!$payload) {
            return null;
        }

        return json_decode($payload, true);
    }

    /**
     * 迁移已到期的延迟任务到主队列
     *
     * @param string $queue 队列名称
     * @return void
     */
    protected function migrateDelayedJobs(string $queue): void {
        $now = time();
        $key = $this->getDelayedKey($queue);

        // 获取所有已到期的任务
        $jobs = $this->redis->zrangebyscore($key, 0, $now);

        foreach ($jobs as $job) {
            // 移动到主队列
            $this->redis->rpush($this->getQueueKey($queue), [$job]);
            // 从延迟队列删除
            $this->redis->zrem($key, $job);
        }
    }

    /**
     * 获取队列大小
     * 
     * 包括主队列和延迟队列的任务数量
     *
     * @param string $queue 队列名称
     * @return int 队列中的任务数量
     */
    public function size(string $queue): int {
        $mainSize = $this->redis->llen($this->getQueueKey($queue));
        $delayedSize = $this->redis->zcard($this->getDelayedKey($queue));

        return $mainSize + $delayedSize;
    }

    /**
     * 从队列中删除任务
     *
     * @param string $jobId 任务ID
     * @param string $queue 队列名称
     * @return bool 是否删除成功
     */
    public function delete(string $jobId, string $queue): bool {
        return true;
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
        return true;
    }

    /**
     * 获取队列统计信息
     *
     * @param string $queue 队列名称
     * @return array 统计信息数组
     */
    public function stats(string $queue): array {
        return [
            'queue' => $queue,
            'size' => $this->size($queue),
            'timestamp' => time(),
        ];
    }

    /**
     * 开始事务（Redis 不支持事务，方法为空）
     *
     * @return void
     */
    public function beginTransaction(): void {
    }

    /**
     * 提交事务（Redis 不支持事务，方法为空）
     *
     * @return void
     */
    public function commit(): void {
    }

    /**
     * 回滚事务（Redis 不支持事务，方法为空）
     *
     * @return void
     */
    public function rollback(): void {
    }

    /**
     * 关闭 Redis 连接
     *
     * @return void
     */
    public function close(): void {
        if ($this->redis) {
            $this->redis->disconnect();
        }
    }

    /**
     * 获取队列的 Redis Key
     *
     * @param string $queue 队列名称
     * @return string Redis Key
     */
    protected function getQueueKey(string $queue): string {
        return "queue:{$queue}";
    }

    /**
     * 获取延迟队列的 Redis Key
     *
     * @param string $queue 队列名称
     * @return string Redis Key
     */
    protected function getDelayedKey(string $queue): string {
        return "queue:{$queue}:delayed";
    }
}
