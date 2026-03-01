<?php

namespace Kode\Queue;

interface QueueInterface {
    /**
     * 推送任务到队列
     *
     * @param mixed  $job 任务名称或闭包
     * @param array  $data 任务数据
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return string 任务ID
     */
    public function push($job, array $data = [], string $queue = null): string;

    /**
     * 推送原始负载到队列
     *
     * @param string $payload 原始负载
     * @param string $queue 队列名称，null 表示使用默认队列
     * @param array  $options 选项
     * @return string 任务ID
     */
    public function pushRaw(string $payload, string $queue = null, array $options = []): string;

    /**
     * 延迟推送任务到队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param mixed  $job 任务名称或闭包
     * @param array  $data 任务数据
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return string 任务ID
     */
    public function later(int $delay, $job, array $data = [], string $queue = null): string;

    /**
     * 批量推送任务到队列
     *
     * @param array  $jobs 任务列表
     * @param array  $data 任务数据
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return array 任务ID列表
     */
    public function bulk(array $jobs, array $data = [], string $queue = null): array;

    /**
     * 从队列中取出下一个任务
     *
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return mixed 任务数据
     */
    public function pop(string $queue = null);

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return int 队列大小
     */
    public function size(string $queue = null): int;

    /**
     * 从队列中删除任务
     *
     * @param string $jobId 任务ID
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return bool 是否删除成功
     */
    public function delete(string $jobId, string $queue = null): bool;

    /**
     * 将任务释放回队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $jobId 任务ID
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return bool 是否释放成功
     */
    public function release(int $delay, string $jobId, string $queue = null): bool;

    /**
     * 获取队列统计信息
     *
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return array 统计信息
     */
    public function stats(string $queue = null): array;

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
     * 获取全局队列实例
     *
     * @param string $queue 队列名称
     * @return QueueInterface 队列实例
     */
    public function global(string $queue = 'default'): QueueInterface;

    /**
     * 获取局部队列实例
     *
     * @param string $queue 队列名称
     * @return QueueInterface 队列实例
     */
    public function local(string $queue = 'default'): QueueInterface;
}
