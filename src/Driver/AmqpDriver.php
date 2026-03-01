<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Channel\AMQPChannel;

class AmqpDriver implements DriverInterface {
    protected $connection;
    protected $channel;
    protected $queue;

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

    public function push(string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $queueName = $queue ?: $this->queue;
        $this->channel->queue_declare($queueName, false, true, false, false);

        $message = new AMQPMessage($payload, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $this->channel->basic_publish($message, '', $queueName);

        return $jobId;
    }

    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $data['available_at'] = time() + $delay;
        $payload = json_encode($data);

        return $this->push($payload, $queue, $options);
    }

    public function pop(string $queue) {
        $queueName = $queue ?: $this->queue;
        $this->channel->queue_declare($queueName, false, true, false, false);

        $message = $this->channel->basic_get($queueName);

        if (!$message) {
            return null;
        }

        $this->channel->basic_ack($message->getDeliveryTag());

        return json_decode($message->body, true);
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
        if ($this->channel) {
            $this->channel->close();
        }
        if ($this->connection) {
            $this->connection->close();
        }
    }
}
