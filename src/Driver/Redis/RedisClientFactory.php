<?php

declare(strict_types=1);

namespace Kode\Queue\Driver\Redis;

use Kode\Queue\Config\ConnectionConfig;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Exception\DriverException;

/**
 * Redis 客户端工厂。
 *
 * 解析顺序：
 * 1. 配置里直接给了现成实例（`client_instance`）→ 直接包装，复用框架已有连接池；
 * 2. 显式指定 `client` = predis / phpredis → 按需构建；
 * 3. `client` = auto（默认）→ 优先 phpredis（C 扩展，性能更好），其次 Predis。
 */
final class RedisClientFactory
{
    private function __construct()
    {
    }

    public static function make(ConnectionConfig $config): RedisClientInterface
    {
        $instance = $config->object('client_instance') ?? $config->object('connection');

        if ($instance !== null) {
            return self::wrap($instance);
        }

        return match ($config->string('client', 'auto')) {
            'predis' => self::predis($config),
            'phpredis', 'ext-redis', 'redis' => self::phpredis($config),
            default => self::auto($config),
        };
    }

    /**
     * 包装外部传入的客户端实例。
     */
    public static function wrap(object $client): RedisClientInterface
    {
        return match (true) {
            $client instanceof RedisClientInterface => $client,
            $client instanceof \Redis => new PhpRedisClient($client),
            default => new PredisClient($client),
        };
    }

    private static function auto(ConnectionConfig $config): RedisClientInterface
    {
        if (\extension_loaded('redis')) {
            return self::phpredis($config);
        }

        if (class_exists(\Predis\Client::class)) {
            return self::predis($config);
        }

        throw DriverException::missingDependency(DriverType::Redis);
    }

    private static function phpredis(ConnectionConfig $config): RedisClientInterface
    {
        if (!\extension_loaded('redis')) {
            throw DriverException::missingDependency(DriverType::Redis);
        }

        $redis = new \Redis();
        $host = $config->string('host', '127.0.0.1') ?? '127.0.0.1';
        $port = $config->int('port', 6379);
        $timeout = $config->float('timeout', 2.0);

        $connected = $config->bool('persistent')
            ? $redis->pconnect($host, $port, $timeout)
            : $redis->connect($host, $port, $timeout);

        if ($connected !== true) {
            throw DriverException::connectionFailed(DriverType::Redis, sprintf('%s:%d 连接失败', $host, $port));
        }

        if (($password = $config->secret('password')) !== null) {
            $user = $config->string('username');
            $redis->auth($user === null || $user === '' ? $password : [$user, $password]);
        }

        if (($database = $config->int('database')) > 0) {
            $redis->select($database);
        }

        // 队列负载已经由 SerializerInterface 处理，禁止客户端二次序列化
        $redis->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_NONE);
        // 前缀由驱动自行拼接，避免与 Lua 脚本里的 KEYS 冲突
        $redis->setOption(\Redis::OPT_PREFIX, '');

        return new PhpRedisClient($redis);
    }

    private static function predis(ConnectionConfig $config): RedisClientInterface
    {
        if (!class_exists(\Predis\Client::class)) {
            throw DriverException::missingDependency(DriverType::Redis);
        }

        $parameters = array_filter([
            'scheme' => $config->string('scheme', 'tcp'),
            'host' => $config->string('host', '127.0.0.1'),
            'port' => $config->int('port', 6379),
            'database' => $config->int('database'),
            'password' => $config->secret('password'),
            'username' => $config->string('username'),
            'timeout' => $config->float('timeout', 2.0),
            'read_write_timeout' => $config->float('read_write_timeout', 0.0),
        ], static fn (mixed $v): bool => $v !== null && $v !== '');

        /** @var array<string, mixed> $options */
        $options = $config->array('options');

        return new PredisClient(new \Predis\Client($parameters, $options));
    }
}
