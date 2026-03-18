<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use Pheanstalk\Pheanstalk;
use Pheanstalk\Job;

/**
 * Beanstalkd 队列驱动
 * 
 * 使用 Beanstalkd 作为队列存储后端，提供高性能、可靠的队列服务。
 * Beanstalkd 是一个轻量级的消息队列系统，专为异步任务处理设计。
 */
class BeanstalkdDriver implements DriverInterface {
    /**
     * Pheanstalk 客户端实例
     *
     * @var Pheanstalk
     */
    protected Pheanstalk $pheanstalk;

    /**
     * 默认管道名称
     *
     * @var string
     */
    protected string $tube;

    /**
     * 创建 Beanstalkd 驱动实例
     *
     * @param array $config 配置数组
     *                       - host: Beanstalkd 服务器地址
     *                       - port: Beanstalkd 端口
     *                       - timeout: 连接超时时间
     *                       - tube: 默认管道名称
     * @throws DriverException Beanstalkd 连接失败时抛出
     */
    public function __construct(array $config = []) {
        try {
            $this->pheanstalk = Pheanstalk::create(
                $config['host'] ?? '127.0.0.1',
                $config['port'] ?? 11300,
                $config['timeout'] ?? 10
            );

            $this->tube = $config['tube'] ?? 'default';
        } catch (\Exception $e) {
            throw new DriverException("Beanstalkd connection failed: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * 推送任务到队列
     *
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称（管道）
     * @param array  $options 额外选项
     * @return string 任务ID
     */
    public function push(string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $this->pheanstalk->useTube($queue ?: $this->tube);
        $this->pheanstalk->put($payload);

        return $jobId;
    }

    /**
     * 延迟推送任务到队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称（管道）
     * @param array  $options 额外选项
     * @return string 任务ID
     */
    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $this->pheanstalk->useTube($queue ?: $this->tube);
        $this->pheanstalk->put($payload, Pheanstalk::DEFAULT_PRIORITY, $delay);

        return $jobId;
    }

    /**
     * 从队列中取出下一个任务
     *
     * @param string $queue 队列名称（管道）
     * @return mixed 任务数据数组，无任务时返回null
     */
    public function pop(string $queue) {
        $this->pheanstalk->watch($queue ?: $this->tube);
        $job = $this->pheanstalk->reserve(0);

        if (!$job) {
            return null;
        }

        // 删除已取出的任务
        $this->pheanstalk->delete($job);

        return json_decode($job->getData(), true);
    }

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称（管道）
     * @return int 队列中的就绪任务数量
     */
    public function size(string $queue): int {
        $stats = $this->pheanstalk->statsTube($queue ?: $this->tube);
        return (int)($stats['current-jobs-ready'] ?? 0);
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
     * @param string $queue 队列名称（管道）
     * @return array 统计信息数组
     */
    public function stats(string $queue): array {
        $stats = $this->pheanstalk->statsTube($queue ?: $this->tube);
        return [
            'queue' => $queue,
            'size' => $stats['current-jobs-ready'] ?? 0,
            'timestamp' => time(),
        ];
    }

    /**
     * 开始事务（Beanstalkd 不支持事务，方法为空）
     *
     * @return void
     */
    public function beginTransaction(): void {
    }

    /**
     * 提交事务（Beanstalkd 不支持事务，方法为空）
     *
     * @return void
     */
    public function commit(): void {
    }

    /**
     * 回滚事务（Beanstalkd 不支持事务，方法为空）
     *
     * @return void
     */
    public function rollback(): void {
    }

    /**
     * 关闭 Beanstalkd 连接
     *
     * @return void
     */
    public function close(): void {
    }
}
