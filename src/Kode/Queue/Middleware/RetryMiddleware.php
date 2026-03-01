<?php

namespace Kode\Queue\Middleware;

use Kode\Queue\Exception\QueueException;

class RetryMiddleware implements MiddlewareInterface {
    /**
     * The maximum number of attempts.
     *
     * @var int
     */
    protected $maxAttempts;

    /**
     * The initial delay in milliseconds.
     *
     * @var int
     */
    protected $delay;

    /**
     * The delay multiplier.
     *
     * @var float
     */
    protected $multiplier;

    /**
     * Create a new retry middleware instance.
     *
     * @param int   $maxAttempts
     * @param int   $delay
     * @param float $multiplier
     */
    public function __construct(int $maxAttempts = 3, int $delay = 100, float $multiplier = 2.0) {
        $this->maxAttempts = $maxAttempts;
        $this->delay = $delay;
        $this->multiplier = $multiplier;
    }

    /**
     * Handle a queue operation.
     *
     * @param callable $next
     * @param string   $method
     * @param array    $parameters
     * @return mixed
     * @throws QueueException
     */
    public function handle(callable $next, string $method, array $parameters) {
        $attempts = 0;
        $currentDelay = $this->delay;

        while (true) {
            try {
                return $next($parameters);
            } catch (QueueException $e) {
                $attempts++;

                if ($attempts >= $this->maxAttempts) {
                    throw $e;
                }

                usleep($currentDelay * 1000); // Convert milliseconds to microseconds
                $currentDelay *= $this->multiplier;
            }
        }
    }
}