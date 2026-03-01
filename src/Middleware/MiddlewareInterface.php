<?php

namespace Kode\Queue\Middleware;

use Kode\Queue\QueueInterface;

interface MiddlewareInterface {
    public function handle(callable $next, string $method, array $parameters);
}
