<?php

namespace Kode\Queue\Driver;

/**
 * 队列驱动接口
 * 
 * 定义所有队列驱动必须实现的方法，确保不同驱动之间的统一接口。
 * 所有驱动必须实现此接口才能与队列系统兼容。
 */
interface DriverInterface {
    /**
     * 推送任务到队列
     *
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称
     * @param array  $options 额外选项
     * @return string 任务ID
     */
    public function push(string $payload, string $queue, array $options = []): string;

    /**
     * 延迟推送任务到队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称
     * @param array  $options 额外选项
     * @return string 任务ID
     */
    public function later(int $delay, string $payload, string $queue, array $options = []): string;

    /**
     * 从队列中取出下一个任务
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据数组，无任务时返回null
     */
    public function pop(string $queue);

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称
     * @return int 队列中的任务数量
     */
    public function size(string $queue): int;

    /**
     * 从队列中删除任务
     *
     * @param string $jobId 任务ID
     * @param string $queue 队列名称
     * @return bool 是否删除成功
     */
    public function delete(string $jobId, string $queue): bool;

    /**
     * 将任务释放回队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $jobId 任务ID
     * @param string $queue 队列名称
     * @return bool 是否释放成功
     */
    public function release(int $delay, string $jobId, string $queue): bool;

    /**
     * 获取队列统计信息
     *
     * @param string $queue 队列名称
     * @return array 统计信息数组
     */
    public function stats(string $queue): array;

    /**
     * 开始事务
     *
     * @return void
     */
    public function beginTransaction(): void;

    /**
     * 提交事务
     *
     * @return void
     */
    public function commit(): void;

    /**
     * 回滚事务
     *
     * @return void
     */
    public function rollback(): void;

    /**
     * 关闭连接
     *
     * @return void
     */
    public function close(): void;
}
