<?php

namespace Kode\Queue\Middleware;

class LogMiddleware implements MiddlewareInterface {
    protected $logger;

    public function __construct(callable $logger = null) {
        $this->logger = $logger ?? function (string $message) {
            echo "[" . date('Y-m-d H:i:s') . "] " . $message . PHP_EOL;
        };
    }

    public function handle(callable $next, string $method, array $parameters) {
        $startTime = microtime(true);
        $logger = $this->logger;

        try {
            $result = $next($parameters);
            $endTime = microtime(true);
            $duration = ($endTime - $startTime) * 1000;

            $logger(sprintf(
                "Queue operation %s completed in %.2fms",
                $method,
                $duration
            ));

            return $result;
        } catch (\Exception $e) {
            $endTime = microtime(true);
            $duration = ($endTime - $startTime) * 1000;

            $logger(sprintf(
                "Queue operation %s failed in %.2fms: %s",
                $method,
                $duration,
                $e->getMessage()
            ));

            throw $e;
        }
    }
}
