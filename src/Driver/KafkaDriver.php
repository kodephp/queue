<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use RdKafka\Producer;
use RdKafka\Consumer;
use RdKafka\TopicConf;
use RdKafka\Conf;

/**
 * Kafka 队列驱动
 * 
 * 使用 Apache Kafka 作为队列存储后端。
 * Kafka 是一个分布式流处理平台，提供高吞吐量、持久化和分布式消息队列。
 * 
 * 注意：需要安装 rdkafka PHP 扩展
 */
class KafkaDriver implements DriverInterface {
    /**
     * Kafka 生产者实例
     *
     * @var Producer
     */
    protected Producer $producer;

    /**
     * Kafka 消费者实例
     *
     * @var Consumer
     */
    protected Consumer $consumer;

    /**
     * 默认主题名称
     *
     * @var string
     */
    protected string $topic;

    /**
     * 消费者组ID
     *
     * @var string
     */
    protected string $groupId;

    /**
     * 配置数组
     *
     * @var array
     */
    protected array $config;

    /**
     * 当前订阅的主题
     *
     * @var string|null
     */
    protected ?string $currentQueue = null;

    /**
     * 创建 Kafka 驱动实例
     *
     * @param array $config 配置数组
     *                       - bootstrap_servers: Kafka 服务器地址列表
     *                       - topic: 默认主题名称
     *                       - group_id: 消费者组ID
     * @throws DriverException rdkafka 扩展未安装或连接失败时抛出
     */
    public function __construct(array $config = []) {
        if (!extension_loaded('rdkafka')) {
            throw new DriverException('Kafka PHP extension (rdkafka) is required for Kafka driver. Please install it using: pecl install rdkafka');
        }

        $this->topic = $config['topic'] ?? 'queue';
        $this->groupId = $config['group_id'] ?? 'queue-consumer';
        $this->config = $config;

        $this->producer = $this->createProducer($config);
        $this->consumer = $this->createConsumer($config);
    }

    /**
     * 创建 Kafka 生产者
     *
     * @param array $config 配置数组
     * @return Producer 生产者实例
     */
    protected function createProducer(array $config): Producer {
        $conf = new Conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers'] ?? '127.0.0.1:9092');
        $conf->set('queue.buffering.max.messages', '1000000');
        $conf->set('message.send.max.retries', '3');
        $conf->set('retry.backoff.ms', '100');

        return new Producer($conf);
    }

    /**
     * 创建 Kafka 消费者
     *
     * @param array $config 配置数组
     * @return Consumer 消费者实例
     */
    protected function createConsumer(array $config): Consumer {
        $conf = new Conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers'] ?? '127.0.0.1:9092');
        $conf->set('group.id', $this->groupId);
        $conf->set('auto.offset.reset', 'earliest');

        return new Consumer($conf);
    }

    /**
     * 推送任务到队列
     * 
     * 将消息发送到 Kafka 主题
     *
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称（主题）
     * @param array  $options 额外选项
     * @return string 任务ID
     */
    public function push(string $payload, string $queue, array $options = []): string {
        $topicName = $queue ?: $this->topic;
        $topic = $this->producer->newTopic($topicName);
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        // 发送消息到分区
        $partition = defined('RD_KAFKA_PARTITION_UA') ? RD_KAFKA_PARTITION_UA : 0;
        $topic->produce($partition, 0, $payload);
        $this->producer->flush(1000);

        return $jobId;
    }

    /**
     * 延迟推送任务到队列
     * 
     * Kafka 本身不支持延迟消息，通过在消息中添加延迟时间戳实现
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $payload 任务负载（JSON格式）
     * @param string $queue 队列名称（主题）
     * @param array  $options 额外选项
     * @return string 任务ID
     */
    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $data['delay_until'] = time() + $delay;
        $payload = json_encode($data);

        return $this->push($payload, $queue, $options);
    }

    /**
     * 从队列中取出下一个任务
     * 
     * 从 Kafka 主题消费消息，会自动处理延迟消息
     *
     * @param string $queue 队列名称（主题）
     * @return mixed 任务数据数组，无任务时返回null
     * @throws DriverException Kafka 错误时抛出
     */
    public function pop(string $queue) {
        $topicName = $queue ?: $this->topic;

        // 切换订阅主题
        if ($this->currentQueue !== $topicName) {
            $this->consumer->subscribe([$topicName]);
            $this->currentQueue = $topicName;
        }

        $message = $this->consumer->consume(1000);

        if (!$message) {
            return null;
        }

        // 处理消息状态
        switch ($message->err) {
            case (defined('RD_KAFKA_RESP_ERR_NO_ERROR') ? RD_KAFKA_RESP_ERR_NO_ERROR : 0):
                $payload = $message->payload;
                $data = json_decode($payload, true);

                // 检查延迟消息是否到期
                if (isset($data['delay_until']) && $data['delay_until'] > time()) {
                    // 未到期，重新入队
                    $this->push($payload, $queue);
                    return null;
                }

                unset($data['delay_until']);

                return $data;
            case (defined('RD_KAFKA_RESP_ERR__PARTITION_EOF') ? RD_KAFKA_RESP_ERR__PARTITION_EOF : -1):
                return null;
            case (defined('RD_KAFKA_RESP_ERR__TIMED_OUT') ? RD_KAFKA_RESP_ERR__TIMED_OUT : -2):
                return null;
            default:
                throw new DriverException('Kafka error: ' . $message->errstr(), $message->err);
        }
    }

    /**
     * 获取队列大小
     * 
     * Kafka 不直接支持获取队列大小，返回 0
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
     * 开始事务（Kafka 不支持事务，方法为空）
     *
     * @return void
     */
    public function beginTransaction(): void {
    }

    /**
     * 提交事务（Kafka 不支持事务，方法为空）
     *
     * @return void
     */
    public function commit(): void {
    }

    /**
     * 回滚事务（Kafka 不支持事务，方法为空）
     *
     * @return void
     */
    public function rollback(): void {
    }

    /**
     * 关闭 Kafka 连接
     *
     * @return void
     */
    public function close(): void {
    }
}
