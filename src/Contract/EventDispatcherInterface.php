<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

/**
 * 事件派发契约。
 *
 * 签名刻意与 PSR-14 保持一致，因此：
 * - 任何 PSR-14 实现可直接注入；
 * - kode/event 的 `Dispatcher` 通过 {@see \Kode\Queue\Integration\KodeEventBridge} 适配；
 * - 不接事件系统时使用 `NullEventDispatcher`，零开销。
 */
interface EventDispatcherInterface
{
    /**
     * @template T of object
     * @param  T $event
     * @return T
     */
    public function dispatch(object $event): object;
}
