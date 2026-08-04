<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Kode\Queue\Contract\MiddlewareInterface;
use Override;

/**
 * 中间件基类：提供默认优先级，子类只需实现 handle()。
 */
abstract class AbstractMiddleware implements MiddlewareInterface
{
    /** 默认优先级 */
    public const int DEFAULT_PRIORITY = 0;

    public function __construct(protected readonly int $priority = self::DEFAULT_PRIORITY)
    {
    }

    #[Override]
    public function priority(): int
    {
        return $this->priority;
    }
}
