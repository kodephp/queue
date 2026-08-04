<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

/**
 * 配置非法：缺少必填项、类型不符、连接名不存在等。
 */
final class InvalidConfigurationException extends QueueException
{
    public static function unknownConnection(string $name, array $available): self
    {
        return new self(
            sprintf('未定义的队列连接 "%s"，已配置：%s', $name, implode('、', $available) ?: '（空）'),
            context: ['connection' => $name, 'available' => $available],
        );
    }

    public static function missingOption(string $connection, string $key): self
    {
        return new self(
            sprintf('连接 "%s" 缺少必填配置项 "%s"', $connection, $key),
            context: ['connection' => $connection, 'key' => $key],
        );
    }

    public static function invalidType(string $key, string $expected, mixed $actual): self
    {
        return new self(
            sprintf('配置项 "%s" 期望 %s，实际为 %s', $key, $expected, get_debug_type($actual)),
            context: ['key' => $key, 'expected' => $expected, 'actual' => get_debug_type($actual)],
        );
    }
}
