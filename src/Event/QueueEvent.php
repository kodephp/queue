<?php

declare(strict_types=1);

namespace Kode\Queue\Event;

/**
 * 队列事件基类。
 *
 * 所有事件均为只读值对象，可安全地跨进程 / 跨协程传递。
 * 事件名同时暴露为常量字符串，便于 kode/event 这类以字符串为键的派发器订阅。
 */
abstract readonly class QueueEvent
{
    public float $timestamp;

    public function __construct()
    {
        $this->timestamp = microtime(true);
    }

    /**
     * 事件名（用于字符串订阅）。
     */
    public function name(): string
    {
        return static::class;
    }

    /**
     * @return array<string, mixed>
     */
    abstract public function toArray(): array;
}
