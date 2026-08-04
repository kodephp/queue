<?php

declare(strict_types=1);

namespace Kode\Queue\Serializer;

use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Exception\InvalidConfigurationException;

/**
 * 序列化器工厂。
 */
final class SerializerFactory
{
    /** @var array<string, callable(): SerializerInterface> */
    private static array $custom = [];

    private function __construct()
    {
    }

    /**
     * 按名称创建。
     *
     * - `json`      ：默认，跨语言可读
     * - `igbinary`  ：体积小、速度快，需扩展
     * - `auto`      ：有 igbinary 就用 igbinary，同时兼容读取 JSON 存量消息
     * - `composite` ：`auto` 的别名，语义更直白（写新格式、读时自动识别）
     */
    public static function make(string $name = 'json'): SerializerInterface
    {
        $name = strtolower(trim($name));

        if (isset(self::$custom[$name])) {
            return (self::$custom[$name])();
        }

        return match ($name) {
            'json', '' => new JsonSerializer(),
            'igbinary' => new IgbinarySerializer(),
            'auto', 'composite' => self::auto(),
            default => throw new InvalidConfigurationException(
                sprintf(
                    '未知的序列化器 "%s"，可用：json、igbinary、auto、composite%s',
                    $name,
                    self::$custom === [] ? '' : '、' . implode('、', array_keys(self::$custom)),
                ),
            ),
        };
    }

    /**
     * 自动挑选最优实现，并保留对旧格式的读兼容。
     */
    public static function auto(): SerializerInterface
    {
        $json = new JsonSerializer();

        if (!\extension_loaded('igbinary')) {
            return $json;
        }

        return new CompositeSerializer(new IgbinarySerializer(), $json);
    }

    /**
     * 注册自定义序列化器（如 MessagePack、Protobuf）。
     *
     * @param callable(): SerializerInterface $factory
     */
    public static function extend(string $name, callable $factory): void
    {
        self::$custom[strtolower(trim($name))] = $factory;
    }
}
