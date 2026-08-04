<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

/**
 * 事务异常：未开启事务时提交、嵌套层级异常、回滚失败等。
 */
final class TransactionException extends QueueException
{
    public static function notActive(string $operation): self
    {
        return new self(
            sprintf('当前没有活动的事务，无法执行 [%s]', $operation),
            context: ['operation' => $operation],
        );
    }

    public static function alreadyActive(): self
    {
        return new self('事务已开启，该驱动不支持嵌套事务');
    }

    public static function rollbackFailed(\Throwable $previous): self
    {
        return new self(
            '事务回滚失败：' . $previous->getMessage(),
            previous: $previous,
        );
    }
}
