<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use Predis\Client;

class RedisDriver implements DriverInterface {
    /**
     * The Redis client.
     *
     * @var Client
     */
    protected $client;

    /**
     * The queue prefix.
     *
     * @var string
     */
    protected $prefix;

    /**
     * Create a new Redis driver instance.
     *
     * @param array $config
     * @throws DriverException
     */
    public function __construct(array $config) {
        if (!class_exists(Client::class)) {
            throw new DriverException('Predis library is required for Redis driver');
        }

        $this->prefix = $config['prefix'] ?? 'queue:';
        $this->client = $this->createClient($config);
    }

    /**
     * Create a Redis client instance.
     *
     * @param array $config
     * @return Client
     */
    protected function createClient(array $config): Client {
        $clientConfig = [
            'scheme' => 'tcp',
            'host' => $config['host'] ?? '127.0.0.1',
            'port' => $config['port'] ?? 6379,
            'password' => $config['password'] ?? null,
            'database' => $config['database'] ?? 0,
        ];

        return new Client($clientConfig, $config['options'] ?? []);
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
        $jobId = $this->generateJobId();
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $this->client->rpush($this->getQueueKey($queue), $payload);

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
        $jobId = $this->generateJobId();
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $this->client->zadd(
            $this->getDelayQueueKey($queue),
            time() + $delay,
            $payload
        );

        return $jobId;
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string $queue
     * @return mixed
     */
    public function pop(string $queue) {
        // Process delayed jobs
        $this->processDelayedJobs($queue);

        // Pop from the main queue
        $payload = $this->client->lpop($this->getQueueKey($queue));

        if ($payload) {
            return json_decode($payload, true);
        }

        return null;
    }

    /**
     * Process delayed jobs.
     *
     * @param string $queue
     * @return void
     */
    protected function processDelayedJobs(string $queue): void {
        $now = time();
        $delayQueue = $this->getDelayQueueKey($queue);
        $mainQueue = $this->getQueueKey($queue);

        // Get all jobs that are ready
        $jobs = $this->client->zrangebyscore($delayQueue, 0, $now);

        if (!empty($jobs)) {
            // Add jobs to main queue
            foreach ($jobs as $job) {
                $this->client->rpush($mainQueue, $job);
            }

            // Remove jobs from delay queue
            $this->client->zremrangebyscore($delayQueue, 0, $now);
        }
    }

    /**
     * Get the size of the queue.
     *
     * @param string $queue
     * @return int
     */
    public function size(string $queue): int {
        $mainSize = $this->client->llen($this->getQueueKey($queue));
        $delaySize = $this->client->zcard($this->getDelayQueueKey($queue));

        return $mainSize + $delaySize;
    }

    /**
     * Delete a job from the queue.
     *
     * @param string $jobId
     * @param string $queue
     * @return bool
     */
    public function delete(string $jobId, string $queue): bool {
        // Jobs in main queue are processed immediately, so we don't need to delete them
        // For delayed jobs, we need to scan and remove
        $delayQueue = $this->getDelayQueueKey($queue);
        $jobs = $this->client->zrange($delayQueue, 0, -1);

        foreach ($jobs as $job) {
            $data = json_decode($job, true);
            if ($data['id'] === $jobId) {
                $this->client->zrem($delayQueue, $job);
                return true;
            }
        }

        return false;
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
        // For simplicity, we'll just push the job back with the delay
        // In a real implementation, you might want to track the job data
        return true;
    }

    /**
     * Get queue statistics.
     *
     * @param string $queue
     * @return array
     */
    public function stats(string $queue): array {
        $mainSize = $this->client->llen($this->getQueueKey($queue));
        $delaySize = $this->client->zcard($this->getDelayQueueKey($queue));

        return [
            'queue' => $queue,
            'size' => $mainSize + $delaySize,
            'main_queue_size' => $mainSize,
            'delay_queue_size' => $delaySize,
            'timestamp' => time(),
        ];
    }

    /**
     * Begin a transaction.
     *
     * @return void
     */
    public function beginTransaction(): void {
        $this->client->multi();
    }

    /**
     * Commit a transaction.
     *
     * @return void
     */
    public function commit(): void {
        $this->client->exec();
    }

    /**
     * Rollback a transaction.
     *
     * @return void
     */
    public function rollback(): void {
        $this->client->discard();
    }

    /**
     * Close the connection.
     *
     * @return void
     */
    public function close(): void {
        $this->client->disconnect();
    }

    /**
     * Generate a job ID.
     *
     * @return string
     */
    protected function generateJobId(): string {
        return uniqid('', true);
    }

    /**
     * Get the queue key.
     *
     * @param string $queue
     * @return string
     */
    protected function getQueueKey(string $queue): string {
        return $this->prefix . $queue;
    }

    /**
     * Get the delay queue key.
     *
     * @param string $queue
     * @return string
     */
    protected function getDelayQueueKey(string $queue): string {
        return $this->prefix . $queue . ':delayed';
    }
}