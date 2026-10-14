<?php

declare(strict_types=1);

namespace Kode\Queue\Integration;

/**
 * kode/context 桥接。
 *
 * 设计原则：**软依赖**。装了 kode/context 就用它（自动支持 Fiber / Swoole 协程 /
 * 多进程 / 多线程的上下文隔离）；没装就退化为一个进程内静态数组，
 * 功能不缺失，只是不具备协程隔离能力。
 *
 * 这样 kode/queue 可以独立发布，也能在 kodephp 全家桶里无缝协同。
 */
final class KodeContextBridge
{
    private const string CONTEXT_CLASS = 'Kode\\Context\\Context';

    /** 未安装 kode/context 时的降级存储 */
    private static array $fallback = [];

    private static ?bool $available = null;

    private function __construct()
    {
    }

    /**
     * kode/context 是否可用。
     */
    public static function isAvailable(): bool
    {
        return self::$available ??= class_exists(self::CONTEXT_CLASS);
    }

    public static function get(string $key, mixed $default = null): mixed
    {
        if (self::isAvailable()) {
            /** @var callable $getter */
            $getter = [self::CONTEXT_CLASS, 'get'];

            return $getter($key, $default);
        }

        return self::$fallback[$key] ?? $default;
    }

    public static function set(string $key, mixed $value): void
    {
        if (self::isAvailable()) {
            /** @var callable $setter */
            $setter = [self::CONTEXT_CLASS, 'set'];
            $setter($key, $value);

            return;
        }

        self::$fallback[$key] = $value;
    }

    public static function has(string $key): bool
    {
        if (self::isAvailable()) {
            /** @var callable $checker */
            $checker = [self::CONTEXT_CLASS, 'has'];

            return (bool) $checker($key);
        }

        return \array_key_exists($key, self::$fallback);
    }

    /**
     * 导出指定键（用于写入任务头）。
     *
     * @param  list<string>         $keys
     * @return array<string, mixed>
     */
    public static function export(array $keys): array
    {
        $out = [];
        foreach ($keys as $key) {
            $value = self::get($key);
            if ($value !== null && (\is_scalar($value) || \is_array($value))) {
                $out[$key] = $value;
            }
        }

        return $out;
    }

    /**
     * 导入上下文（消费端还原链路信息）。
     *
     * $scopeKeys 中未被本次数据覆盖的键会被显式清空：常驻 worker 顺序消费
     * 多个任务时，不清空会让上一个任务的 tenant_id / user_id 残留给下一个。
     *
     * @param array<string, mixed> $data
     * @param list<string>         $scopeKeys
     */
    public static function import(array $data, array $scopeKeys = []): void
    {
        foreach ($data as $key => $value) {
            if (\is_string($key) && $key !== 'queued_at') {
                self::set($key, $value);
            }
        }

        foreach ($scopeKeys as $key) {
            if (\is_string($key) && !\array_key_exists($key, $data)) {
                self::set($key, null);
            }
        }
    }

    /**
     * 在隔离的上下文中执行回调（继承当前上下文，退出时回滚处理器写入）。
     *
     * 常驻 worker 顺序消费多个任务时，任务处理器经 Context::set 写入的业务键
     * 若不回收会脏读给下一个任务。语义取 **fork**（继承 + 退出回滚）而非 run
     * （空白作用域）：ContextMiddleware 在 pop 时刚导入的 trace_id / tenant_id
     * 必须对处理器可见，但处理器新增的键不得外泄。
     *
     * 优先 Context::fork()（kode/context ≥ 3.2）；旧版回退手工 copy/restore；
     * 未安装 context 时回退进程内静态数组快照。
     *
     * @template T
     * @param  callable(): T $callback
     * @return T
     */
    public static function scoped(callable $callback): mixed
    {
        if (self::isAvailable()) {
            if (method_exists(self::CONTEXT_CLASS, 'fork')) {
                /** @var callable $runner */
                $runner = [self::CONTEXT_CLASS, 'fork'];

                /** @var T */
                return $runner($callback);
            }

            /** @var callable $copy */
            $copy = [self::CONTEXT_CLASS, 'copy'];
            /** @var callable $restore */
            $restore = [self::CONTEXT_CLASS, 'restore'];
            $snapshot = $copy();

            try {
                return $callback();
            } finally {
                $restore($snapshot);
            }
        }

        $snapshot = self::$fallback;

        try {
            return $callback();
        } finally {
            self::$fallback = $snapshot;
        }
    }

    /**
     * 当前 trace id（没有则返回 null）。
     */
    public static function traceId(): ?string
    {
        $value = self::get('trace_id');

        return \is_string($value) && $value !== '' ? $value : null;
    }

    /**
     * 清空降级存储（仅测试用）。
     */
    public static function reset(): void
    {
        self::$fallback = [];
        self::$available = null;
    }
}
