<?php

namespace Kode\Queue\Middleware;

use Kode\Queue\QueueInterface;

interface MiddlewareInterface {
    /**
     * Handle a queue operation.
     *
     * @param callable $next
     * @param string   $method
     * @param array    $parameters
     * @return mixed
     */
    public function handle(callable $next, string $method, array $parameters);
}