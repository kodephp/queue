<?php

declare(strict_types=1);

namespace Kode\Queue\Serializer;

use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Exception\SerializationException;
use Kode\Queue\Message\Job;
use Override;

/**
 * igbinary 序列化器。
 *
 * 相同负载体积约为 JSON 的 60%~70%，编解码快 2 倍左右，
 * 适合内网、纯 PHP 消费者、高 QPS 场景。带 4 字节魔数前缀便于与 JSON 负载共存。
 */
final class IgbinarySerializer implements SerializerInterface
{
    public const string NAME = 'igbinary';

    /** 魔数前缀，用于混合负载识别 */
    public const string MAGIC = "\x00KQI";

    public function __construct()
    {
        if (!\extension_loaded('igbinary')) {
            throw new SerializationException(
                'igbinary 扩展未安装，请执行 pecl install igbinary 或改用 JsonSerializer',
                context: ['serializer' => self::NAME],
            );
        }
    }

    #[Override]
    public function name(): string
    {
        return self::NAME;
    }

    #[Override]
    public function serialize(Job $job): string
    {
        $encoded = igbinary_serialize($job->toArray());

        if ($encoded === null) {
            throw SerializationException::encodeFailed(
                new \RuntimeException('igbinary_serialize 返回 null'),
                self::NAME,
            );
        }

        return self::MAGIC . $encoded;
    }

    #[Override]
    public function deserialize(string $raw): Job
    {
        if (!$this->supports($raw)) {
            throw SerializationException::decodeFailed($raw, self::NAME);
        }

        /** @var array<string, mixed>|null $data */
        $data = @igbinary_unserialize(substr($raw, \strlen(self::MAGIC)));

        if (!\is_array($data)) {
            throw SerializationException::decodeFailed($raw, self::NAME);
        }

        return Job::fromArray($data);
    }

    #[Override]
    public function supports(string $raw): bool
    {
        return str_starts_with($raw, self::MAGIC);
    }
}
