<?php

declare(strict_types=1);

namespace Kode\Queue\Config;

use Kode\Queue\Enum\DriverType;
use Kode\Queue\Exception\InvalidConfigurationException;

/**
 * 全局队列配置（不可变）。
 *
 * 支持三种来源，按需选用：
 * - 数组：`QueueConfig::fromArray([...])`
 * - 环境变量：`QueueConfig::fromEnv()`（12-Factor 部署）
 * - 零配置：`QueueConfig::auto()`（自动挑选当前环境可用的最佳驱动）
 */
final readonly class QueueConfig
{
    /**
     * @param string                          $default     默认连接名
     * @param array<string, ConnectionConfig> $connections 连接表
     * @param string|null                     $failedStore 失败任务存储连接名
     */
    public function __construct(
        public private(set) string $default,
        public array $connections,
        public ?string $failedStore = null,
    ) {
        if (!isset($this->connections[$this->default])) {
            throw InvalidConfigurationException::unknownConnection(
                $this->default,
                array_keys($this->connections),
            );
        }
    }

    /**
     * @param array{default?: string, connections?: array<string, array<string, mixed>>, failed_store?: string|null} $config
     */
    public static function fromArray(array $config): self
    {
        $rawConnections = $config['connections'] ?? [];

        if ($rawConnections === []) {
            // 只给了 driver 之类的扁平配置：视为单连接
            $rawConnections = ['default' => $config];
            $default = 'default';
        } else {
            $default = (string) ($config['default'] ?? array_key_first($rawConnections));
        }

        $connections = [];
        foreach ($rawConnections as $name => $options) {
            $connections[(string) $name] = ConnectionConfig::fromArray((string) $name, (array) $options);
        }

        return new self($default, $connections, $config['failed_store'] ?? null);
    }

    /**
     * 从环境变量读取（QUEUE_DRIVER / QUEUE_HOST / QUEUE_PORT ...）。
     */
    public static function fromEnv(string $prefix = 'QUEUE_'): self
    {
        $get = static function (string $key, mixed $default = null) use ($prefix): mixed {
            $name = $prefix . $key;
            $value = $_ENV[$name] ?? getenv($name);

            // 只有「未设置」(getenv 返回 false) 与空串才回落默认值；
            // "0" 是合法配置（如端口、数据库序号），不能被 `?:` 吞掉。
            return $value === false || $value === '' ? $default : $value;
        };

        $driver = (string) $get('DRIVER', DriverType::DEFAULT->value);

        return self::fromArray([
            'default' => $driver,
            'connections' => [
                $driver => array_filter([
                    'driver' => $driver,
                    'host' => $get('HOST'),
                    'port' => $get('PORT'),
                    'password' => $get('PASSWORD'),
                    'database' => $get('DATABASE'),
                    'dsn' => $get('DSN'),
                    'username' => $get('USERNAME'),
                    'table' => $get('TABLE'),
                    'queue' => $get('NAME', 'default'),
                    'serializer' => $get('SERIALIZER', 'json'),
                    'retry_after' => $get('RETRY_AFTER'),
                ], static fn (mixed $v): bool => $v !== null && $v !== false && $v !== ''),
            ],
        ]);
    }

    /**
     * 零配置：按 Redis → Database → Memory 的顺序挑选第一个可用驱动。
     */
    public static function auto(): self
    {
        $preferred = [DriverType::Redis, DriverType::Database, DriverType::Memory];

        foreach ($preferred as $type) {
            if ($type->isAvailable()) {
                return self::fromArray([
                    'default' => $type->value,
                    'connections' => [$type->value => ['driver' => $type->value]],
                ]);
            }
        }

        return self::fromArray([
            'default' => 'memory',
            'connections' => ['memory' => ['driver' => 'memory']],
        ]);
    }

    /**
     * 取连接配置。
     */
    public function connection(?string $name = null): ConnectionConfig
    {
        $name ??= $this->default;

        return $this->connections[$name]
            ?? throw InvalidConfigurationException::unknownConnection($name, $this->names());
    }

    public function has(string $name): bool
    {
        return isset($this->connections[$name]);
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        return array_keys($this->connections);
    }

    /**
     * 追加 / 覆盖一个连接，返回新实例。
     */
    public function withConnection(ConnectionConfig $connection): self
    {
        return new self(
            $this->default,
            [...$this->connections, $connection->name => $connection],
            $this->failedStore,
        );
    }

    public function withDefault(string $name): self
    {
        return new self($name, $this->connections, $this->failedStore);
    }

    /**
     * @return array<string, mixed>
     */
    public function toSafeArray(): array
    {
        return [
            'default' => $this->default,
            'failed_store' => $this->failedStore,
            'connections' => array_map(
                static fn (ConnectionConfig $c): array => $c->toSafeArray(),
                $this->connections,
            ),
        ];
    }
}
