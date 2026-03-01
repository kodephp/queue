<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use RdKafka\Producer;
use RdKafka\Consumer;
use RdKafka\TopicConf;
use RdKafka\Conf;

class KafkaDriver implements DriverInterface {
    protected $producer;
    protected $consumer;
    protected $topic;
    protected $groupId;
    protected $config;
    protected $currentQueue;

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

    protected function createProducer(array $config): Producer {
        $conf = new Conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers'] ?? '127.0.0.1:9092');
        $conf->set('queue.buffering.max.messages', '1000000');
        $conf->set('message.send.max.retries', '3');
        $conf->set('retry.backoff.ms', '100');

        return new Producer($conf);
    }

    protected function createConsumer(array $config): Consumer {
        $conf = new Conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers'] ?? '127.0.0.1:9092');
        $conf->set('group.id', $this->groupId);
        $conf->set('auto.offset.reset', 'earliest');

        return new Consumer($conf);
    }

    public function push(string $payload, string $queue, array $options = []): string {
        $topicName = $queue ?: $this->topic;
        $topic = $this->producer->newTopic($topicName);
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $partition = defined('RD_KAFKA_PARTITION_UA') ? RD_KAFKA_PARTITION_UA : 0;
        $topic->produce($partition, 0, $payload);
        $this->producer->flush(1000);

        return $jobId;
    }

    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $data['delay_until'] = time() + $delay;
        $payload = json_encode($data);

        return $this->push($payload, $queue, $options);
    }

    public function pop(string $queue) {
        $topicName = $queue ?: $this->topic;

        if ($this->currentQueue !== $topicName) {
            $this->consumer->subscribe([$topicName]);
            $this->currentQueue = $topicName;
        }

        $message = $this->consumer->consume(1000);

        if (!$message) {
            return null;
        }

        switch ($message->err) {
            case (defined('RD_KAFKA_RESP_ERR_NO_ERROR') ? RD_KAFKA_RESP_ERR_NO_ERROR : 0):
                $payload = $message->payload;
                $data = json_decode($payload, true);

                if (isset($data['delay_until']) && $data['delay_until'] > time()) {
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

    public function size(string $queue): int {
        return 0;
    }

    public function delete(string $jobId, string $queue): bool {
        return true;
    }

    public function release(int $delay, string $jobId, string $queue): bool {
        return true;
    }

    public function stats(string $queue): array {
        return [
            'queue' => $queue,
            'size' => $this->size($queue),
            'timestamp' => time(),
        ];
    }

    public function beginTransaction(): void {
    }

    public function commit(): void {
    }

    public function rollback(): void {
    }

    public function close(): void {
    }
}
