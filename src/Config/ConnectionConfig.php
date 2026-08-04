<?php

declare(strict_types=1);

namespace Kode\Queue\Config;

use Kode\Queue\Enum\DriverType;
use Kode\Queue\Exception\InvalidConfigurationException;
use SensitiveParameter;

/**
 * 单个队列连接的配置（不可变）。
 *
 * v1 把 `array $config` 一路透传到驱动构造函数，写错 key 只会得到默认值，
 * 且 IDE 无法补全。v2 用值对象 + 类型化访问器把这些问题挡在入口。
 */
final readonly class ConnectionConfig
{
    /**
     * @param string               $name       连接名
     * @param DriverType           $driver     驱动类型
     * @param array<string, mixed> $options    驱动参数（已与默认值合并）
     * @param string               $queue      该连接的默认队列名
     * @param string               $serializer 序列化器名
     * @param int                  $retryAfter 可见性超时（秒）：预留超过该时长视为 Worker 崩溃
     */
    public function __construct(
        public string $name,
        public DriverType $driver,
        public array $options = [],
        public string $queue = 'default',
        public string $serializer = 'json',
        public int $retryAfter = 90,
    ) {
    }

    /**
     * 从松散数组构建，自动合并驱动默认值。
     *
     * @param array<string, mixed> $config
     */
    public static function fromArray(string $name, array $config): self
    {
        $driverName = (string) ($config['driver'] ?? $name);
        $driver = DriverType::tryFromName($driverName)
            ?? throw new InvalidConfigurationException(
                sprintf('连接 "%s" 指定了未知驱动 "%s"', $name, $driverName),
                context: ['connection' => $name, 'driver' => $driverName],
            );

        $options = [...$driver->defaultConfig(), ...$config];
        unset($options['driver'], $options['serializer'], $options['retry_after']);

        return new self(
            name: $name,
            driver: $driver,
            options: $options,
            // Beanstalkd 用 tube，其他用 queue
            queue: (string) ($config['queue'] ?? $config['tube'] ?? 'default'),
            serializer: (string) ($config['serializer'] ?? 'json'),
            retryAfter: (int) ($config['retry_after'] ?? $driver->defaultConfig()['retry_after'] ?? 90),
        );
    }

    // ------------------------------------------------------------------
    // 类型化访问器：读错类型立刻报错，而不是把 null 传进驱动
    // ------------------------------------------------------------------

    public function string(string $key, ?string $default = null): ?string
    {
        $value = $this->options[$key] ?? $default;

        return $value === null ? null : (string) $value;
    }

    public function requireString(string $key): string
    {
        return $this->string($key)
            ?? throw InvalidConfigurationException::missingOption($this->name, $key);
    }

    public function int(string $key, int $default = 0): int
    {
        $value = $this->options[$key] ?? $default;

        if (!is_numeric($value)) {
            throw InvalidConfigurationException::invalidType("{$this->name}.{$key}", 'int', $value);
        }

        return (int) $value;
    }

    public function float(string $key, float $default = 0.0): float
    {
        $value = $this->options[$key] ?? $default;

        if (!is_numeric($value)) {
            throw InvalidConfigurationException::invalidType("{$this->name}.{$key}", 'float', $value);
        }

        return (float) $value;
    }

    public function bool(string $key, bool $default = false): bool
    {
        return filter_var(
            $this->options[$key] ?? $default,
            FILTER_VALIDATE_BOOLEAN,
            FILTER_NULL_ON_FAILURE,
        ) ?? $default;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function array(string $key, array $default = []): array
    {
        $value = $this->options[$key] ?? $default;

        return \is_array($value) ? $value : $default;
    }

    /**
     * 敏感值（密码 / token）。标注 #[SensitiveParameter] 后，
     * 该参数不会出现在异常堆栈里，避免密码被日志泄露。
     */
    public function secret(#[SensitiveParameter] string $key, ?string $default = null): ?string
    {
        $value = $this->options[$key] ?? $default;

        return $value === null || $value === '' ? $default : (string) $value;
    }

    public function object(string $key): ?object
    {
        $value = $this->options[$key] ?? null;

        return \is_object($value) ? $value : null;
    }

    public function has(string $key): bool
    {
        return \array_key_exists($key, $this->options);
    }

    /**
     * 覆盖部分选项，返回新实例。
     *
     * @param array<string, mixed> $options
     */
    public function with(array $options): self
    {
        return new self(
            name: $this->name,
            driver: $this->driver,
            options: [...$this->options, ...$options],
            queue: $this->queue,
            serializer: $this->serializer,
            retryAfter: $this->retryAfter,
        );
    }

    /**
     * 导出为数组，密码等敏感字段自动脱敏（用于日志与诊断命令）。
     *
     * @return array<string, mixed>
     */
    public function toSafeArray(): array
    {
        $masked = [];
        foreach ($this->options as $key => $value) {
            $masked[$key] = preg_match('/pass|secret|token|key$/i', $key) === 1 && $value !== null
                ? '***'
                : $value;
        }

        return [
            'name' => $this->name,
            'driver' => $this->driver->value,
            'queue' => $this->queue,
            'serializer' => $this->serializer,
            'retry_after' => $this->retryAfter,
            'options' => $masked,
        ];
    }
}
