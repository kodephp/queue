<?php

namespace Kode\Queue\Driver;

use RdKafka\Producer;
use RdKafka\Consumer;
use RdKafka\TopicConf;
use RdKafka\Conf;

class KafkaDriver implements DriverInterface {
    /**
     * Kafka 生产者
     *
     * @var Producer
     */
    protected $producer;

    /**
     * Kafka 消费者
     *
     * @var Consumer
     */
    protected $consumer;

    /**
     * 主题名称
     *
     * @var string
     */
    protected $topic;

    /**
     * 消费者组
     *
     * @var string
     */
    protected $groupId;

    /**
     * 配置
     *
     * @var array
     */
    protected $config;

    /**
     * 创建新的 Kafka 驱动实例
     *
     * @param array $config 配置
     */
    public function __construct(array $config = []) {
        if (!extension_loaded('rdkafka')) {
            throw new \RuntimeException('Kafka PHP extension (rdkafka) is required for Kafka driver. Please install it using: pecl install rdkafka');
        }

        if (!class_exists(Producer::class)) {
            throw new \RuntimeException('Kafka PHP extension (rdkafka) is installed but not properly configured');
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
     * @param array $config 配置
     * @return Producer
     */
    protected function createProducer(array $config): Producer {
        $conf = new Conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers'] ?? '127.0.0.1:9092');

        $producer = new Producer($conf);
        return $producer;
    }

    /**
     * 创建 Kafka 消费者
     *
     * @param array $config 配置
     * @return Consumer
     */
    protected function createConsumer(array $config): Consumer {
        $conf = new Conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers'] ?? '127.0.0.1:9092');
        $conf->set('group.id', $this->groupId);
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new Consumer($conf);
        $consumer->subscribe([$this->topic]);

        return $consumer;
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
        $topicName = $queue ?: $this->topic;
        $topic = $this->producer->newTopic($topicName);
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        // 使用正确的 Kafka 常量
        $partition = defined('RD_KAFKA_PARTITION_UA') ? RD_KAFKA_PARTITION_UA : 0;
        $topic->produce($partition, 0, $payload);
        $this->producer->flush(1000);

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
        // Kafka 不原生支持延迟消息，这里我们使用时间戳标记
        $data = json_decode($payload, true);
        $data['delay_until'] = time() + $delay;
        $payload = json_encode($data);

        return $this->push($payload, $queue, $options);
    }

    /**
     * 从队列中取出下一个任务
     *
     * @param string $queue 队列名称
     * @return mixed 任务数据
     */
    public function pop(string $queue) {
        // 如果队列名称与默认主题不同，重新订阅
        $topicName = $queue ?: $this->topic;
        if ($topicName !== $this->topic) {
            $this->consumer->unsubscribe();
            $this->consumer->subscribe([$topicName]);
        }

        $message = $this->consumer->consume(1000);

        switch ($message->err) {
            case (defined('RD_KAFKA_RESP_ERR_NO_ERROR') ? RD_KAFKA_RESP_ERR_NO_ERROR : 0):
                $payload = $message->payload;
                $data = json_decode($payload, true);

                // 检查是否是延迟消息
                if (isset($data['delay_until']) && $data['delay_until'] > time()) {
                    // 消息还未到时间，重新入队
                    $this->push($payload, $queue);
                    return null;
                }

                // 移除延迟标记
                unset($data['delay_until']);

                return $data;
            case (defined('RD_KAFKA_RESP_ERR__PARTITION_EOF') ? RD_KAFKA_RESP_ERR__PARTITION_EOF : -1):
                return null;
            case (defined('RD_KAFKA_RESP_ERR__TIMED_OUT') ? RD_KAFKA_RESP_ERR__TIMED_OUT : -2):
                return null;
            default:
                throw new \RuntimeException('Kafka error: ' . $message->errstr(), $message->err);
        }
    }

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称
     * @return int 队列大小
     */
    public function size(string $queue): int {
        // Kafka 不直接支持获取队列大小，这里返回 0 作为占位符
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
        // Kafka 是基于日志的系统，不支持直接删除消息
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
        // Kafka 不支持直接释放消息，我们可以重新推送
        return true;
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
            'size' => 0,
            'timestamp' => time(),
        ];
    }

    /**
     * 开始事务
     *
     * @return void
     */
    public function beginTransaction(): void {
        // Kafka 支持事务，但这里简化处理
    }

    /**
     * 提交事务
     *
     * @return void
     */
    public function commit(): void {
        // Kafka 支持事务，但这里简化处理
    }

    /**
     * 回滚事务
     *
     * @return void
     */
    public function rollback(): void {
        // Kafka 支持事务，但这里简化处理
    }

    /**
     * 关闭连接
     *
     * @return void
     */
    public function close(): void {
        // Kafka 客户端会在对象销毁时自动关闭
    }
}