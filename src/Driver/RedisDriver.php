<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use Predis\Client;

class RedisDriver implements DriverInterface {
    protected $redis;
    protected $connection;

    public function __construct(array $config = []) {
        try {
            $this->redis = new Client([
                'scheme' => $config['scheme'] ?? 'tcp',
                'host' => $config['host'] ?? '127.0.0.1',
                'port' => $config['port'] ?? 6379,
                'database' => $config['database'] ?? 0,
                'password' => $config['password'] ?? null,
            ] + ($config['options'] ?? []));

            $this->connection = $config;
        } catch (\Exception $e) {
            throw new DriverException("Redis connection failed: " . $e->getMessage(), 0, $e);
        }
    }

    public function push(string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $this->redis->rpush($this->getQueueKey($queue), [$payload]);

        return $jobId;
    }

    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $data['available_at'] = time() + $delay;
        $payload = json_encode($data);

        $this->redis->zadd($this->getDelayedKey($queue), time() + $delay, $payload);

        return $jobId;
    }

    public function pop(string $queue) {
        $this->migrateDelayedJobs($queue);

        $payload = $this->redis->lpop($this->getQueueKey($queue));

        if (!$payload) {
            return null;
        }

        return json_decode($payload, true);
    }

    protected function migrateDelayedJobs(string $queue): void {
        $now = time();
        $key = $this->getDelayedKey($queue);

        $jobs = $this->redis->zrangebyscore($key, 0, $now);

        foreach ($jobs as $job) {
            $this->redis->rpush($this->getQueueKey($queue), [$job]);
            $this->redis->zrem($key, $job);
        }
    }

    public function size(string $queue): int {
        $mainSize = $this->redis->llen($this->getQueueKey($queue));
        $delayedSize = $this->redis->zcard($this->getDelayedKey($queue));

        return $mainSize + $delayedSize;
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
        if ($this->redis) {
            $this->redis->disconnect();
        }
    }

    protected function getQueueKey(string $queue): string {
        return "queue:{$queue}";
    }

    protected function getDelayedKey(string $queue): string {
        return "queue:{$queue}:delayed";
    }
}
