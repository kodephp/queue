<?php

declare(strict_types=1);

namespace Kode\Queue\Enum;

/**
 * 任务优先级。
 *
 * 内部统一使用「数值越大越优先」的直觉语义，
 * 由枚举负责翻译成各后端的实际取值（Beanstalkd 反而是越小越优先）。
 */
enum Priority: int
{
    case Lowest = 0;
    case Low = 25;
    case Normal = 50;
    case High = 75;
    case Highest = 100;

    /** Beanstalkd 默认优先级基准值 */
    public const int BEANSTALKD_BASE = 1024;

    /** AMQP 优先级上限（x-max-priority 常用值） */
    public const int AMQP_MAX = 10;

    /**
     * 从任意整数归一化到最接近的优先级档位。
     */
    public static function fromScore(int $score): self
    {
        return match (true) {
            $score <= 12 => self::Lowest,
            $score <= 37 => self::Low,
            $score <= 62 => self::Normal,
            $score <= 87 => self::High,
            default => self::Highest,
        };
    }

    /**
     * 转换为 Beanstalkd 优先级（0 最优先，越小越先执行）。
     */
    public function toBeanstalkd(): int
    {
        return (int) round(self::BEANSTALKD_BASE * (1 - $this->value / 100));
    }

    /**
     * 转换为 AMQP 优先级（0-10，越大越优先）。
     */
    public function toAmqp(): int
    {
        return (int) round(self::AMQP_MAX * $this->value / 100);
    }

    /**
     * 转换为 Redis Sorted Set 排序分值（越小越先出队）。
     */
    public function toRedisScore(float $timestamp): float
    {
        // 用优先级把时间戳做偏移：高优先级相当于「提前」到达
        return $timestamp - $this->value * 10;
    }

    public function label(): string
    {
        return match ($this) {
            self::Lowest => '最低',
            self::Low => '低',
            self::Normal => '普通',
            self::High => '高',
            self::Highest => '最高',
        };
    }
}
