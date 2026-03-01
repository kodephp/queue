<?php

namespace Kode\Queue\Driver;

interface DriverInterface {
    /**
     * Push a job onto the queue.
     *
     * @param string $payload
     * @param string $queue
     * @param array  $options
     * @return string
     */
    public function push(string $payload, string $queue, array $options = []): string;

    /**
     * Push a job onto the queue after a delay.
     *
     * @param int    $delay
     * @param string $payload
     * @param string $queue
     * @param array  $options
     * @return string
     */
    public function later(int $delay, string $payload, string $queue, array $options = []): string;

    /**
     * Pop the next job off of the queue.
     *
     * @param string $queue
     * @return mixed
     */
    public function pop(string $queue);

    /**
     * Get the size of the queue.
     *
     * @param string $queue
     * @return int
     */
    public function size(string $queue): int;

    /**
     * Delete a job from the queue.
     *
     * @param string $jobId
     * @param string $queue
     * @return bool
     */
    public function delete(string $jobId, string $queue): bool;

    /**
     * Release a job back onto the queue.
     *
     * @param int    $delay
     * @param string $jobId
     * @param string $queue
     * @return bool
     */
    public function release(int $delay, string $jobId, string $queue): bool;

    /**
     * Get queue statistics.
     *
     * @param string $queue
     * @return array
     */
    public function stats(string $queue): array;

    /**
     * Begin a transaction.
     *
     * @return void
     */
    public function beginTransaction(): void;

    /**
     * Commit a transaction.
     *
     * @return void
     */
    public function commit(): void;

    /**
     * Rollback a transaction.
     *
     * @return void
     */
    public function rollback(): void;

    /**
     * Close the connection.
     *
     * @return void
     */
    public function close(): void;
}