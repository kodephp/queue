<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

use Kode\Queue\Middleware\Operation;

/**
 * 队列操作中间件契约（生产侧 / 驱动侧）。
 *
 * v1 的签名是 `handle(callable $next, string $method, array $parameters)`——
 * 参数是无类型的位置数组，中间件想读「当前队列名」只能靠 `$parameters[1]` 这种魔法索引。
 * v2 把它收敛成一个 {@see Operation} 值对象，语义自解释且可静态分析。
 */
interface MiddlewareInterface
{
    /**
     * @param Operation                   $operation 当前操作
     * @param callable(Operation): mixed  $next      调用链的下一环
     */
    public function handle(Operation $operation, callable $next): mixed;

    /**
     * 优先级，数值越大越靠外层（越先执行）。
     */
    public function priority(): int;
}
