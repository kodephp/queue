<?php

declare(strict_types=1);

namespace Kode\Queue\Enum;

/**
 * 驱动能力枚举。
 *
 * 队列后端能力差异极大（Kafka 没有延迟投递、Redis 没有原生优先级……）。
 * 通过统一的能力探测，上层可以在运行前就明确降级策略，而不是等到抛异常。
 *
 * @see \Kode\Queue\Contract\DriverInterface::supports()
 */
enum Capability: string
{
    /** 延迟投递 */
    case Delay = 'delay';

    /** 优先级投递 */
    case Priority = 'priority';

    /** 批量投递 */
    case Batch = 'batch';

    /** 消息确认（ack / delete） */
    case Ack = 'ack';

    /** 重新入队（release / nack requeue） */
    case Release = 'release';

    /** 死信 / 埋葬（bury） */
    case Bury = 'bury';

    /** 清空队列 */
    case Clear = 'clear';

    /** 队列长度统计 */
    case Size = 'size';

    /** 阻塞式拉取（长轮询） */
    case Blocking = 'blocking';

    /** 事务 */
    case Transaction = 'transaction';

    /** 窥视队首（不消费） */
    case Peek = 'peek';

    public function label(): string
    {
        return match ($this) {
            self::Delay => '延迟投递',
            self::Priority => '优先级',
            self::Batch => '批量投递',
            self::Ack => '消息确认',
            self::Release => '重新入队',
            self::Bury => '死信埋葬',
            self::Clear => '清空队列',
            self::Size => '长度统计',
            self::Blocking => '阻塞拉取',
            self::Transaction => '事务',
            self::Peek => '队首窥视',
        };
    }
}
