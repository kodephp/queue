<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

use Kode\Queue\Contract\EventDispatcherInterface;
use Override;

/**
 * 空派发器：不接事件系统时的默认实现，调用开销接近于零。
 */
final class NullEventDispatcher implements EventDispatcherInterface
{
    private static ?self $instance = null;

    public static function instance(): self
    {
        return self::$instance ??= new self();
    }

    #[Override]
    public function dispatch(object $event): object
    {
        return $event;
    }
}
