<?php

namespace Kode\Queue\Driver;

interface DriverInterface {
    public function push(string $payload, string $queue, array $options = []): string;
    public function later(int $delay, string $payload, string $queue, array $options = []): string;
    public function pop(string $queue);
    public function size(string $queue): int;
    public function delete(string $jobId, string $queue): bool;
    public function release(int $delay, string $jobId, string $queue): bool;
    public function stats(string $queue): array;
    public function beginTransaction(): void;
    public function commit(): void;
    public function rollback(): void;
    public function close(): void;
}
