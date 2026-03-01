<?php

namespace Kode\Queue\Middleware;

use Kode\Queue\Exception\QueueException;

class RetryMiddleware implements MiddlewareInterface {
    protected int $maxAttempts;
    protected int $delay;
    protected float $multiplier;

    public function __construct(int $maxAttempts = 3, int $delay = 100, float $multiplier = 1.0) {
        $this->maxAttempts = $maxAttempts;
        $this->delay = $delay;
        $this->multiplier = $multiplier;
    }

    public function handle(callable $next, string $method, array $parameters) {
        $attempts = 0;
        $delay = $this->delay;

        while ($attempts < $this->maxAttempts) {
            try {
                return $next($parameters);
            } catch (\Exception $e) {
                $attempts++;
                
                if ($attempts >= $this->maxAttempts) {
                    throw new QueueException(
                        "Max retry attempts ({$this->maxAttempts}) reached: " . $e->getMessage(),
                        0,
                        $e
                    );
                }

                usleep($delay * 1000);
                $delay = (int)($delay * $this->multiplier);
            }
        }
    }
}
