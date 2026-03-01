<?php

namespace Kode\Queue\Driver;

use Kode\Queue\Exception\DriverException;
use Pheanstalk\Pheanstalk;
use Pheanstalk\Job;

class BeanstalkdDriver implements DriverInterface {
    protected $pheanstalk;
    protected $tube;

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

    public function push(string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $this->pheanstalk->useTube($queue ?: $this->tube);
        $this->pheanstalk->put($payload);

        return $jobId;
    }

    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $payload = json_encode($data);

        $this->pheanstalk->useTube($queue ?: $this->tube);
        $this->pheanstalk->put($payload, Pheanstalk::DEFAULT_PRIORITY, $delay);

        return $jobId;
    }

    public function pop(string $queue) {
        $this->pheanstalk->watch($queue ?: $this->tube);
        $job = $this->pheanstalk->reserve(0);

        if (!$job) {
            return null;
        }

        $this->pheanstalk->delete($job);

        return json_decode($job->getData(), true);
    }

    public function size(string $queue): int {
        $stats = $this->pheanstalk->statsTube($queue ?: $this->tube);
        return (int)($stats['current-jobs-ready'] ?? 0);
    }

    public function delete(string $jobId, string $queue): bool {
        return true;
    }

    public function release(int $delay, string $jobId, string $queue): bool {
        return true;
    }

    public function stats(string $queue): array {
        $stats = $this->pheanstalk->statsTube($queue ?: $this->tube);
        return [
            'queue' => $queue,
            'size' => $stats['current-jobs-ready'] ?? 0,
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
