<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Channel\AMQPChannel;

class AmqpDriver implements DriverInterface {
    /**
     * The AMQP connection.
     *
     * @var AMQPStreamConnection
     */
    protected $connection;

    /**
     * The AMQP channel.
     *
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * The default queue name.
     *
     * @var string
     */
    protected $defaultQueue;

    /**
     * Create a new AMQP driver instance.
     *
     * @param array $config
     * @throws DriverException
     */
    public function __construct(array $config) {
        if (!class_exists(AMQPStreamConnection::class)) {
            throw new DriverException('php-amqplib library is required for AMQP driver');
        }

        $host = $config['host'] ?? '127.0.0.1';
        $port = $config['port'] ?? 5672;
        $username = $config['username'] ?? 'guest';
        $password = $config['password'] ?? 'guest';
        $vhost = $config['vhost'] ?? '/';
        $this->defaultQueue = $config['queue'] ?? 'default';

        $this->connection = new AMQPStreamConnection($host, $port, $username, $password, $vhost);
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($this->defaultQueue, false, true, false, false);
    }

    /**
     * Push a job onto the queue.
     *
     * @param string $payload
     * @param string $queue
     * @param array  $options
     * @return string
     */
    public function push(string $payload, string $queue, array $options = []): string {
        $queueName = $queue ?? $this->defaultQueue;
        $jobId = $this->generateJobId();
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $message = new AMQPMessage($payload, [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'message_id' => $jobId,
        ]);

        $this->channel->basic_publish($message, '', $queueName);

        return $jobId;
    }

    /**
     * Push a job onto the queue after a delay.
     *
     * @param int    $delay
     * @param string $payload
     * @param string $queue
     * @param array  $options
     * @return string
     */
    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $queueName = $queue ?? $this->defaultQueue;
        $jobId = $this->generateJobId();
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        // AMQP doesn't support delay natively, we'll use expiration
        $message = new AMQPMessage($payload, [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'message_id' => $jobId,
            'expiration' => $delay * 1000, // Expiration in milliseconds
        ]);

        $this->channel->basic_publish($message, '', $queueName);

        return $jobId;
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string $queue
     * @return mixed
     */
    public function pop(string $queue) {
        $queueName = $queue ?? $this->defaultQueue;
        $message = null;

        $callback = function (AMQPMessage $msg) use (&$message) {
            $message = $msg;
        };

        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);

        // Wait for a message
        while (count($this->channel->callbacks)) {
            $this->channel->wait(null, false, 1); // 1 second timeout
            if ($message) {
                break;
            }
        }

        if ($message) {
            $payload = $message->getBody();
            $data = json_decode($payload, true);
            $data['id'] = $message->get('message_id');
            $data['message'] = $message;

            return $data;
        }

        return null;
    }

    /**
     * Get the size of the queue.
     *
     * @param string $queue
     * @return int
     */
    public function size(string $queue): int {
        $queueName = $queue ?? $this->defaultQueue;
        list($queue, $messageCount, $consumerCount) = $this->channel->queue_declare($queueName, true);

        return $messageCount;
    }

    /**
     * Delete a job from the queue.
     *
     * @param string $jobId
     * @param string $queue
     * @return bool
     */
    public function delete(string $jobId, string $queue): bool {
        // AMQP doesn't support deleting messages by ID directly
        // We'll just return true as a placeholder
        return true;
    }

    /**
     * Release a job back onto the queue.
     *
     * @param int    $delay
     * @param string $jobId
     * @param string $queue
     * @return bool
     */
    public function release(int $delay, string $jobId, string $queue): bool {
        // AMQP doesn't support releasing messages by ID directly
        // We'll just return true as a placeholder
        return true;
    }

    /**
     * Get queue statistics.
     *
     * @param string $queue
     * @return array
     */
    public function stats(string $queue): array {
        $queueName = $queue ?? $this->defaultQueue;
        list($queue, $messageCount, $consumerCount) = $this->channel->queue_declare($queueName, true);

        return [
            'queue' => $queueName,
            'size' => $messageCount,
            'consumer_count' => $consumerCount,
            'timestamp' => time(),
        ];
    }

    /**
     * Begin a transaction.
     *
     * @return void
     */
    public function beginTransaction(): void {
        $this->channel->tx_select();
    }

    /**
     * Commit a transaction.
     *
     * @return void
     */
    public function commit(): void {
        $this->channel->tx_commit();
    }

    /**
     * Rollback a transaction.
     *
     * @return void
     */
    public function rollback(): void {
        $this->channel->tx_rollback();
    }

    /**
     * Close the connection.
     *
     * @return void
     */
    public function close(): void {
        $this->channel->close();
        $this->connection->close();
    }

    /**
     * Generate a job ID.
     *
     * @return string
     */
    protected function generateJobId(): string {
        return uniqid('', true);
    }
}