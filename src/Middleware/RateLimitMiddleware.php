<?php

namespace Kode\Queue\Middleware;

class RateLimitMiddleware implements MiddlewareInterface {
    protected int $capacity;
    protected float $rate;
    protected float $tokens;
    protected float $lastUpdate;

    public function __construct(int $capacity = 10, float $rate = 1.0) {
        $this->capacity = $capacity;
        $this->rate = $rate;
        $this->tokens = $capacity;
        $this->lastUpdate = microtime(true);
    }

    public function handle(callable $next, string $method, array $parameters) {
        $this->refillTokens();

        if ($this->tokens < 1.0) {
            $waitTime = (1.0 - $this->tokens) / $this->rate;
            usleep((int)($waitTime * 1000000));
            $this->refillTokens();
        }

        $this->tokens -= 1.0;

        return $next($parameters);
    }

    protected function refillTokens(): void {
        $now = microtime(true);
        $elapsed = $now - $this->lastUpdate;
        $tokensToAdd = $elapsed * $this->rate;
        
        $this->tokens = min($this->capacity, $this->tokens + $tokensToAdd);
        $this->lastUpdate = $now;
    }
}
