<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

use Kode\Queue\Enum\DriverType;

/**
 * 驱动层异常：连接失败、协议错误、后端不可用等。
 */
class DriverException extends QueueException
{
    /**
     * 依赖缺失。
     */
    public static function missingDependency(DriverType $type): self
    {
        return new self(
            sprintf(
                '驱动 [%s] 的依赖未安装，请执行：%s',
                $type->label(),
                $type->installHint(),
            ),
            context: ['driver' => $type->value, 'requirements' => $type->requirements()],
        );
    }

    /**
     * 连接失败。
     */
    public static function connectionFailed(DriverType $type, \Throwable $previous): self
    {
        return new self(
            sprintf('驱动 [%s] 连接失败：%s', $type->label(), $previous->getMessage()),
            previous: $previous,
            context: ['driver' => $type->value],
        );
    }

    /**
     * 操作失败。
     *
     * @param array<string, mixed> $context
     */
    public static function operationFailed(
        string $operation,
        \Throwable $previous,
        array $context = [],
    ): self {
        return new self(
            sprintf('队列操作 [%s] 执行失败：%s', $operation, $previous->getMessage()),
            previous: $previous,
            context: ['operation' => $operation, ...$context],
        );
    }
}
