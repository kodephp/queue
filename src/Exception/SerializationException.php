<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

/**
 * 负载序列化 / 反序列化失败。
 */
final class SerializationException extends QueueException
{
    public static function encodeFailed(\Throwable $previous, string $serializer): self
    {
        return new self(
            sprintf('任务负载序列化失败（%s）：%s', $serializer, $previous->getMessage()),
            previous: $previous,
            context: ['serializer' => $serializer],
        );
    }

    public static function decodeFailed(string $raw, string $serializer, ?\Throwable $previous = null): self
    {
        return new self(
            sprintf('任务负载反序列化失败（%s）', $serializer),
            previous: $previous,
            context: [
                'serializer' => $serializer,
                'preview' => mb_substr($raw, 0, 256),
                'length' => strlen($raw),
            ],
        );
    }
}
