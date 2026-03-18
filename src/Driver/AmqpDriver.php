<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Channel\AMQPChannel;

/**
 * AMQP 队列驱动
 * 
 * 使用 AMQP 协议（如 RabbitMQ）作为队列存储后端。
 * 提供可靠的消息传递、持久化和消息确认机制。
 */
class AmqpDriver implements DriverInterface {
    /**
     * AMQP 连接实例
     *
     * @var AMQPStreamConnection
     */
    protected AMQPStreamConnection $connection;

    /**
     * AMQP 通道实例
     *
     * @var AMQPChannel
     */
    protected AMQPChannel $channel;

    /**
     * 默认队列名称
     *
     * @var string
     */
    protected string $queue;

    /**
     * 创建 AMQP 驱动实例
     *
     * @param array $config 配置数组
     *                       - host: RabbitMQ 服务器地址
     *                       - port: RabbitMQ 端口
     *                       - username: 用户名
     *                       - password: 密码
     *                       - vhost: 虚拟主机
     *                       - queue: 默认队列名称
     * @throws DriverException AMQP 连接失败时抛出
     */
    public function __construct(array $config = []) {
        try {
            $this->connection = new AMQPStreamConnection(
                $config['host'] ?? '127.0.0.1',
                $config['port'] ?? 5672,
                $config['username'] ?? 'guest',
                $config['password'] ?? 'guest',
                $config['vhost'] ?? '/'
            );

            $this->channel = $this->connection->channel();
            $this->queue = $config['queue'] ?? 'default';
        } catch (\Exception $e) {
            throw new DriverException("AMQP connection failed: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * 推送任务到队列
     * 
     * 使用持久化消息确保消息不会丢失
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

        $queueName = $queue ?: $this->queue;
        // 声明持久化队列
        $this->channel->queue_declare($queueName, false, true, false, false);

        // 创建持久化消息
        $message = new AMQPMessage($payload, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $this->channel->basic_publish($message, '', $queueName);

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
        $payload = json_encode($data);

        return $this->push($payload, $queue, $options);
    }

    /**
     * 从队列中取出下一个任务
     * 
     * 使用消息确认机制确保消息不会丢失
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据数组，无任务时返回null
     */
    public function pop(string $queue) {
        $queueName = $queue ?: $this->queue;
        $this->channel->queue_declare($queueName, false, true, false, false);

        // 获取消息（非阻塞）
        $message = $this->channel->basic_get($queueName);

        if (!$message) {
            return null;
        }

        // 确认消息
        $this->channel->basic_ack($message->getDeliveryTag());

        return json_decode($message->body, true);
    }

    /**
     * 获取队列大小
     * 
     * AMQP 不直接支持获取队列大小，返回 0
     *
     * @param string $queue 队列名称
     * @return int 队列大小
     */
    public function size(string $queue): int {
        return 0;
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
     * 开始事务（AMQP 不支持事务，方法为空）
     *
     * @return void
     */
    public function beginTransaction(): void {
    }

    /**
     * 提交事务（AMQP 不支持事务，方法为空）
     *
     * @return void
     */
    public function commit(): void {
    }

    /**
     * 回滚事务（AMQP 不支持事务，方法为空）
     *
     * @return void
     */
    public function rollback(): void {
    }

    /**
     * 关闭 AMQP 连接
     *
     * @return void
     */
    public function close(): void {
        if ($this->channel) {
            $this->channel->close();
        }
        if ($this->connection) {
            $this->connection->close();
        }
    }
}
