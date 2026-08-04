<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

use Kode\Queue\Message\ReservedJob;

/**
 * 任务执行中间件契约（消费侧 / Worker 侧）。
 *
 * 与 {@see MiddlewareInterface} 分开建模：
 * 前者包裹「对队列的操作」（push/pop/size…），后者包裹「一次任务的执行」。
 * 二者关注点不同，混在一起会导致中间件必须写一堆 `if ($method === 'pop')`。
 */
interface JobMiddlewareInterface
{
    /**
     * @param ReservedJob                   $job  待执行任务
     * @param callable(ReservedJob): mixed  $next 调用链的下一环
     */
    public function handle(ReservedJob $job, callable $next): mixed;

    /**
     * 优先级，数值越大越靠外层。
     */
    public function priority(): int;
}
