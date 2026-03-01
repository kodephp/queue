<?php

namespace Kode\Queue\Driver;

class SyncDriver implements DriverInterface {
    protected $queue = [];
    protected $delayedQueue = [];

    public function push(string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        
        if (!isset($this->queue[$queue])) {
            $this->queue[$queue] = [];
        }
        
        $this->queue[$queue][] = $data;
        
        return $jobId;
    }

    public function later(int $delay, string $payload, string $queue, array $options = []): string {
        $jobId = uniqid('', true);
        $data = json_decode($payload, true);
        $data['id'] = $jobId;
        $data['available_at'] = time() + $delay;
        
        if (!isset($this->delayedQueue[$queue])) {
            $this->delayedQueue[$queue] = [];
        }
        
        $this->delayedQueue[$queue][] = $data;
        
        return $jobId;
    }

    public function pop(string $queue) {
        if (isset($this->delayedQueue[$queue])) {
            $now = time();
            foreach ($this->delayedQueue[$queue] as $key => $job) {
                if ($job['available_at'] <= $now) {
                    unset($job['available_at']);
                    $this->queue[$queue][] = $job;
                    unset($this->delayedQueue[$queue][$key]);
                }
            }
        }

        if (!isset($this->queue[$queue]) || empty($this->queue[$queue])) {
            return null;
        }

        return array_shift($this->queue[$queue]);
    }

    public function size(string $queue): int {
        $mainSize = isset($this->queue[$queue]) ? count($this->queue[$queue]) : 0;
        $delayedSize = isset($this->delayedQueue[$queue]) ? count($this->delayedQueue[$queue]) : 0;
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
    }
}
