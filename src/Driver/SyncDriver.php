<?php

namespace Kode\Queue\Driver;

/**
 * 同步队列驱动
 * 
 * 任务在推送时立即执行，适用于测试环境或不需要异步处理的场景。
 * 任务存储在内存中，不支持持久化。
 */
class SyncDriver implements DriverInterface {
    /**
     * 任务队列存储
     *
     * @var array
     */
    protected array $queue = [];

    /**
     * 延迟任务队列存储
     *
     * @var array
     */
    protected array $delayedQueue = [];

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
        
        if (!isset($this->queue[$queue])) {
            $this->queue[$queue] = [];
        }
        
        $this->queue[$queue][] = $data;
        
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
        $data['available_at'] = time() + $delay;
        
        if (!isset($this->delayedQueue[$queue])) {
            $this->delayedQueue[$queue] = [];
        }
        
        $this->delayedQueue[$queue][] = $data;
        
        return $jobId;
    }

    /**
     * 从队列中取出下一个任务
     * 
     * 会自动处理已到期的延迟任务
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据数组，无任务时返回null
     */
    public function pop(string $queue) {
        // 处理已到期的延迟任务
        if (isset($this->delayedQueue[$queue])) {
            $now = time();
            foreach ($this->delayedQueue[$queue] as $key => $job) {
                if ($job['available_at'] <= $now) {
                    unset($job['available_at']);
                    $this->queue[$queue][] = $job;
                    unset($this->delayedQueue[$queue][$key]);
                }
            }
        }

        if (!isset($this->queue[$queue]) || empty($this->queue[$queue])) {
            return null;
        }

        return array_shift($this->queue[$queue]);
    }

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称
     * @return int 队列中的任务数量（包括延迟任务）
     */
    public function size(string $queue): int {
        $mainSize = isset($this->queue[$queue]) ? count($this->queue[$queue]) : 0;
        $delayedSize = isset($this->delayedQueue[$queue]) ? count($this->delayedQueue[$queue]) : 0;
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
     * 开始事务（同步驱动不支持事务，方法为空）
     *
     * @return void
     */
    public function beginTransaction(): void {
    }

    /**
     * 提交事务（同步驱动不支持事务，方法为空）
     *
     * @return void
     */
    public function commit(): void {
    }

    /**
     * 回滚事务（同步驱动不支持事务，方法为空）
     *
     * @return void
     */
    public function rollback(): void {
    }

    /**
     * 关闭连接（同步驱动无需关闭连接，方法为空）
     *
     * @return void
     */
    public function close(): void {
    }
}
