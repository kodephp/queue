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

    /**
     * @param bool $allowedClasses 是否允许负载实例化对象类。
     *                              默认 false——与 JsonSerializer 的「纯数据」
     *                              语义对齐（Job 字段本就声明为 array/scalar），
     *                              同时封死经 broker 投递的反序列化对象注入。
     *                              传 true 恢复旧版对象复活行为（仅限可信内网负载）。
     */
    public function __construct(private readonly bool $allowedClasses = false)
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

        $blob = substr($raw, \strlen(self::MAGIC));

        /** @var array<string, mixed>|null $data */
        $data = self::optionsSupported()
            ? @igbinary_unserialize($blob, ['allowed_classes' => $this->allowedClasses]) // @phpstan-ignore arguments.count (igbinary 3.2.6+ 支持第二参 options，phpstorm-stubs 未收录)
            : @igbinary_unserialize($blob); // 旧版 igbinary 无第二参（3.2.6 前）

        if (!\is_array($data)) {
            throw SerializationException::decodeFailed($raw, self::NAME);
        }

        // 白名单模式下残留的 __PHP_Incomplete_Class 说明负载里夹带了类——
        // 与其让任务带着垃圾载荷跑进业务，不如在入口干净地拒绝。
        if (! $this->allowedClasses && self::containsIncomplete($data)) {
            throw SerializationException::decodeFailed($raw, self::NAME);
        }

        return Job::fromArray($data);
    }

    /** igbinary >= 3.2.6 才支持 ['allowed_classes' => ...] 选项（静态探测一次） */
    private static function optionsSupported(): bool
    {
        static $supported = null;
        if ($supported === null) {
            try {
                $supported = (new \ReflectionFunction('igbinary_unserialize'))->getNumberOfParameters() >= 2;
            } catch (\ReflectionException) {
                $supported = false;
            }
        }

        return $supported;
    }

    private static function containsIncomplete(mixed $value): bool
    {
        if ($value instanceof \__PHP_Incomplete_Class) {
            return true;
        }
        if (\is_array($value)) {
            return array_any($value, fn($item) => self::containsIncomplete($item));
        }

        return false;
    }

    #[Override]
    public function supports(string $raw): bool
    {
        return str_starts_with($raw, self::MAGIC);
    }
}
