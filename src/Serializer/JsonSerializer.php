<?php

declare(strict_types=1);

namespace Kode\Queue\Serializer;

use JsonException;
use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Exception\SerializationException;
use Kode\Queue\Message\Job;
use Override;

/**
 * JSON 序列化器（默认）。
 *
 * v1 的 `json_encode()` 从不检查返回值，负载里出现非 UTF-8 字节时会静默变成 `false`，
 * 然后 `rpush(false)` 把空串塞进队列——一个极难排查的线上事故。
 *
 * v2 全程 `JSON_THROW_ON_ERROR`，并用 PHP 8.3 新增的 `json_validate()`
 * 做零分配的合法性预检（比 `json_decode() === null` 少一次完整解析 + 对象构造）。
 */
final class JsonSerializer implements SerializerInterface
{
    public const string NAME = 'json';

    /** 默认编码标志：保留中文原文、保留斜杠、非法字符抛异常 */
    public const int DEFAULT_FLAGS = JSON_UNESCAPED_UNICODE
        | JSON_UNESCAPED_SLASHES
        | JSON_PRESERVE_ZERO_FRACTION
        | JSON_THROW_ON_ERROR;

    public function __construct(
        private readonly int $flags = self::DEFAULT_FLAGS,
        private readonly int $depth = 512,
    ) {
    }

    #[Override]
    public function name(): string
    {
        return self::NAME;
    }

    #[Override]
    public function serialize(Job $job): string
    {
        try {
            return json_encode($job->toArray(), $this->flags, $this->depth);
        } catch (JsonException $e) {
            throw SerializationException::encodeFailed($e, self::NAME);
        }
    }

    #[Override]
    public function deserialize(string $raw): Job
    {
        // PHP 8.3：只做语法校验，不构造 PHP 值，内存与耗时都显著低于 json_decode
        if (!json_validate($raw, $this->depth)) {
            throw SerializationException::decodeFailed($raw, self::NAME);
        }

        try {
            /** @var array<string, mixed> $data */
            $data = json_decode($raw, true, $this->depth, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw SerializationException::decodeFailed($raw, self::NAME, $e);
        }

        if (!\is_array($data)) {
            throw SerializationException::decodeFailed($raw, self::NAME);
        }

        return Job::fromArray($data);
    }

    #[Override]
    public function supports(string $raw): bool
    {
        $raw = ltrim($raw);

        return ($raw[0] ?? '') === '{' && json_validate($raw, $this->depth);
    }
}
