<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use Pheanstalk\Pheanstalk;
use Pheanstalk\Job;

class BeanstalkdDriver implements DriverInterface {
    /**
     * The Beanstalkd client.
     *
     * @var Pheanstalk
     */
    protected $client;

    /**
     * The default tube.
     *
     * @var string
     */
    protected $defaultTube;

    /**
     * Create a new Beanstalkd driver instance.
     *
     * @param array $config
     * @throws DriverException
     */
    public function __construct(array $config) {
        if (!class_exists(Pheanstalk::class)) {
            throw new DriverException('Pheanstalk library is required for Beanstalkd driver');
        }

        $host = $config['host'] ?? '127.0.0.1';
        $port = $config['port'] ?? 11300;
        $this->defaultTube = $config['tube'] ?? 'default';

        $this->client = Pheanstalk::create($host, $port);
        $this->client->useTube($this->defaultTube);
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
        $tube = $queue ?? $this->defaultTube;
        $priority = $options['priority'] ?? Pheanstalk::DEFAULT_PRIORITY;
        $delay = $options['delay'] ?? 0;
        $ttr = $options['ttr'] ?? Pheanstalk::DEFAULT_TTR;

        $jobId = $this->client->useTube($tube)->put($payload, $priority, $delay, $ttr);

        return (string) $jobId;
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
        $tube = $queue ?? $this->defaultTube;
        $priority = $options['priority'] ?? Pheanstalk::DEFAULT_PRIORITY;
        $ttr = $options['ttr'] ?? Pheanstalk::DEFAULT_TTR;

        $jobId = $this->client->useTube($tube)->put($payload, $priority, $delay, $ttr);

        return (string) $jobId;
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string $queue
     * @return mixed
     */
    public function pop(string $queue) {
        $tube = $queue ?? $this->defaultTube;

        $job = $this->client->watch($tube)->ignore('default')->reserveWithTimeout(0);

        if ($job instanceof Job) {
            $payload = $job->getData();
            $data = json_decode($payload, true);
            $data['id'] = $job->getId();
            $data['job'] = $job;

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
        $tube = $queue ?? $this->defaultTube;
        $stats = $this->client->statsTube($tube);

        return $stats['current-jobs-ready'] + $stats['current-jobs-delayed'] + $stats['current-jobs-reserved'];
    }

    /**
     * Delete a job from the queue.
     *
     * @param string $jobId
     * @param string $queue
     * @return bool
     */
    public function delete(string $jobId, string $queue): bool {
        try {
            $this->client->delete((int) $jobId);
            return true;
        } catch (\Exception $e) {
            return false;
        }
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
        try {
            $this->client->release((int) $jobId, Pheanstalk::DEFAULT_PRIORITY, $delay);
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * Get queue statistics.
     *
     * @param string $queue
     * @return array
     */
    public function stats(string $queue): array {
        $tube = $queue ?? $this->defaultTube;
        $stats = $this->client->statsTube($tube);

        return [
            'queue' => $tube,
            'size' => $stats['current-jobs-ready'] + $stats['current-jobs-delayed'] + $stats['current-jobs-reserved'],
            'ready_jobs' => $stats['current-jobs-ready'],
            'delayed_jobs' => $stats['current-jobs-delayed'],
            'reserved_jobs' => $stats['current-jobs-reserved'],
            'total_jobs' => $stats['total-jobs'],
            'timestamp' => time(),
        ];
    }

    /**
     * Begin a transaction.
     *
     * @return void
     */
    public function beginTransaction(): void {
        // Beanstalkd doesn't support transactions natively
        // We'll just do nothing here
    }

    /**
     * Commit a transaction.
     *
     * @return void
     */
    public function commit(): void {
        // Beanstalkd doesn't support transactions natively
        // We'll just do nothing here
    }

    /**
     * Rollback a transaction.
     *
     * @return void
     */
    public function rollback(): void {
        // Beanstalkd doesn't support transactions natively
        // We'll just do nothing here
    }

    /**
     * Close the connection.
     *
     * @return void
     */
    public function close(): void {
        // Pheanstalk doesn't have a close method
        // We'll just do nothing here
    }
}