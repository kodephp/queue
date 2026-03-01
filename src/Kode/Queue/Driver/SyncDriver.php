<?php

namespace Kode\Queue\Driver;

class SyncDriver implements DriverInterface {
    /**
     * 任务队列
     *
     * @var array
     */
    protected $queue = [];

    /**
     * 创建新的同步驱动实例
     *
     * @param array $config 配置
     */
    public function __construct(array $config = []) {
        // 同步驱动不需要特殊配置
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
        $jobId = uniqid('', true);
        $this->queue[] = [
            'id' => $jobId,
            'payload' => $payload,
            'queue' => $queue,
            'created_at' => time(),
        ];

        return $jobId;
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
        // 同步驱动不支持延迟，直接推送
        return $this->push($payload, $queue, $options);
    }

    /**
     * 从队列中取出下一个任务
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据
     */
    public function pop(string $queue) {
        foreach ($this->queue as $key => $job) {
            if ($job['queue'] === $queue) {
                $payload = json_decode($job['payload'], true);
                $payload['id'] = $job['id'];
                unset($this->queue[$key]);
                return $payload;
            }
        }

        return null;
    }

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称
     * @return int 队列大小
     */
    public function size(string $queue): int {
        $count = 0;
        foreach ($this->queue as $job) {
            if ($job['queue'] === $queue) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * 从队列中删除任务
     *
     * @param string $jobId 任务ID
     * @param string $queue 队列名称
     * @return bool 是否删除成功
     */
    public function delete(string $jobId, string $queue): bool {
        foreach ($this->queue as $key => $job) {
            if ($job['id'] === $jobId && $job['queue'] === $queue) {
                unset($this->queue[$key]);
                return true;
            }
        }

        return false;
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
        // 同步驱动直接重新推送
        foreach ($this->queue as $key => $job) {
            if ($job['id'] === $jobId && $job['queue'] === $queue) {
                $this->push($job['payload'], $queue);
                unset($this->queue[$key]);
                return true;
            }
        }

        return false;
    }

    /**
     * 获取队列统计信息
     *
     * @param string $queue 队列名称
     * @return array 统计信息
     */
    public function stats(string $queue): array {
        return [
            'queue' => $queue,
            'size' => $this->size($queue),
            'timestamp' => time(),
        ];
    }

    /**
     * 开始事务
     *
     * @return void
     */
    public function beginTransaction(): void {
        // 同步驱动不需要事务
    }

    /**
     * 提交事务
     *
     * @return void
     */
    public function commit(): void {
        // 同步驱动不需要事务
    }

    /**
     * 回滚事务
     *
     * @return void
     */
    public function rollback(): void {
        // 同步驱动不需要事务
    }

    /**
     * 关闭连接
     *
     * @return void
     */
    public function close(): void {
        // 同步驱动不需要关闭连接
    }
}