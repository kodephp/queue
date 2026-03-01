<?php

namespace Kode\Queue\Middleware;

class RateLimitMiddleware implements MiddlewareInterface {
    /**
     * The maximum number of tokens in the bucket.
     *
     * @var int
     */
    protected $capacity;

    /**
     * The rate at which tokens are added to the bucket per second.
     *
     * @var float
     */
    protected $rate;

    /**
     * The current number of tokens in the bucket.
     *
     * @var float
     */
    protected $tokens;

    /**
     * The last time tokens were added to the bucket.
     *
     * @var float
     */
    protected $lastRefillTime;

    /**
     * Create a new rate limit middleware instance.
     *
     * @param int   $capacity
     * @param float $rate
     */
    public function __construct(int $capacity = 10, float $rate = 1.0) {
        $this->capacity = $capacity;
        $this->rate = $rate;
        $this->tokens = $capacity;
        $this->lastRefillTime = microtime(true);
    }

    /**
     * Handle a queue operation.
     *
     * @param callable $next
     * @param string   $method
     * @param array    $parameters
     * @return mixed
     */
    public function handle(callable $next, string $method, array $parameters) {
        $this->refillTokens();

        if ($this->tokens < 1) {
            $this->waitForToken();
        }

        $this->tokens--;

        return $next($parameters);
    }

    /**
     * Refill tokens in the bucket.
     *
     * @return void
     */
    protected function refillTokens(): void {
        $now = microtime(true);
        $elapsed = $now - $this->lastRefillTime;

        $tokensToAdd = $elapsed * $this->rate;
        $this->tokens = min($this->capacity, $this->tokens + $tokensToAdd);
        $this->lastRefillTime = $now;
    }

    /**
     * Wait for a token to become available.
     *
     * @return void
     */
    protected function waitForToken(): void {
        $timeNeeded = (1 - $this->tokens) / $this->rate;
        usleep((int) ($timeNeeded * 1000000)); // Convert seconds to microseconds
        $this->refillTokens();
    }
}