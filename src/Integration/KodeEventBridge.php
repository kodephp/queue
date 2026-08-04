<?php

declare(strict_types=1);

namespace Kode\Queue\Integration;

use Kode\Queue\Contract\EventDispatcherInterface;
use Kode\Queue\Event\QueueEvent;
use Override;

/**
 * 事件派发桥接。
 *
 * 一层薄适配，同时兼容三类派发器：
 * - **kode/event** 的 `Kode\Event\Dispatcher`（`dispatch(object|string, array)`）
 * - **PSR-14** 的 `EventDispatcherInterface`（`dispatch(object)`）
 * - 任意带 `dispatch()` 方法的对象（鸭子类型）
 *
 * 对 kode/event 会额外用「事件类名 + 短名」双通道派发一次，
 * 这样监听者既能 `listen(JobFailed::class, …)` 也能 `listen('job.failed', …)`。
 */
final class KodeEventBridge implements EventDispatcherInterface
{
    /** kode/event 的短名映射：类名 → 点分事件名 */
    private const array SHORT_NAMES = [
        'JobQueued' => 'job.queued',
        'JobProcessing' => 'job.processing',
        'JobProcessed' => 'job.processed',
        'JobFailed' => 'job.failed',
        'WorkerStarting' => 'worker.starting',
        'WorkerStopped' => 'worker.stopped',
    ];

    private readonly bool $isKodeDispatcher;

    public function __construct(private readonly object $dispatcher)
    {
        if (!method_exists($dispatcher, 'dispatch')) {
            throw new \InvalidArgumentException(
                sprintf('事件派发器 %s 必须提供 dispatch() 方法', $dispatcher::class),
            );
        }

        $this->isKodeDispatcher = str_starts_with($dispatcher::class, 'Kode\\Event\\')
            || method_exists($dispatcher, 'listen');
    }

    /**
     * 自动探测：优先用传入的派发器，未提供时尝试新建 kode/event 的 Dispatcher，
     * 都不可用则返回空派发器。
     */
    public static function detect(?object $dispatcher = null): EventDispatcherInterface
    {
        if ($dispatcher instanceof EventDispatcherInterface) {
            return $dispatcher;
        }

        if ($dispatcher !== null) {
            return new self($dispatcher);
        }

        $class = 'Kode\\Event\\Dispatcher';

        return class_exists($class)
            ? new self(new $class())
            : \Kode\Queue\Event\NullEventDispatcher::instance();
    }

    #[Override]
    public function dispatch(object $event): object
    {
        $this->dispatcher->dispatch($event);

        // kode/event 支持字符串事件名，额外派发一次短名，方便按业务语义订阅
        if ($this->isKodeDispatcher && $event instanceof QueueEvent) {
            $short = self::SHORT_NAMES[self::baseName($event::class)] ?? null;
            if ($short !== null) {
                $this->dispatcher->dispatch($short, $event->toArray());
            }
        }

        return $event;
    }

    /**
     * 底层派发器（需要直接注册监听时用）。
     */
    public function raw(): object
    {
        return $this->dispatcher;
    }

    private static function baseName(string $class): string
    {
        $position = strrpos($class, '\\');

        return $position === false ? $class : substr($class, $position + 1);
    }
}
