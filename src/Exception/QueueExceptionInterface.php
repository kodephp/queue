<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

use Throwable;

/**
 * 队列异常标记接口。
 *
 * 调用方可以只 catch 这一个接口来拦截本组件抛出的全部异常，
 * 而不必关心具体的异常类继承结构。
 */
interface QueueExceptionInterface extends Throwable
{
    /**
     * @return array<string, mixed>
     */
    public function context(): array;
}
