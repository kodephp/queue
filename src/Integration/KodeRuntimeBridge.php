<?php

declare(strict_types=1);

namespace Kode\Queue\Integration;

/**
 * 运行时桥接（对接 **kode/runtime** / **kode/fibers** / Swoole / Swow）。
 *
 * Worker 的空转休眠是个容易被忽视的坑：在协程环境里调用 `usleep()`
 * 会阻塞整个线程的调度器，把「一个进程跑 1000 个协程」直接打回「串行执行」。
 *
 * 本桥接负责挑选当前运行时下正确的让出方式：
 * Swoole → `Coroutine::sleep`；Swow → `msleep`；Fiber → `Fiber::suspend`；
 * 都没有 → `usleep`。
 */
final class KodeRuntimeBridge
{
    private static ?string $runtime = null;

    private function __construct()
    {
    }

    /**
     * 当前运行时标识：swoole | swow | fiber | cli
     */
    public static function detect(): string
    {
        if (self::$runtime !== null) {
            return self::$runtime;
        }

        // 优先信任 kode/context 的判定（它同时考虑了进程 / 线程 / 协程）
        if (class_exists('Kode\\Context\\Context')
            && method_exists('Kode\\Context\\Context', 'getRuntime')) {
            /** @var callable $getter */
            $getter = ['Kode\\Context\\Context', 'getRuntime'];
            $runtime = strtolower((string) $getter());
            if ($runtime !== '') {
                return self::$runtime = $runtime;
            }
        }

        return self::$runtime = match (true) {
            \extension_loaded('swoole') && self::inSwooleCoroutine() => 'swoole',
            \extension_loaded('swow') => 'swow',
            \Fiber::getCurrent() !== null => 'fiber',
            default => 'cli',
        };
    }

    /**
     * 是否运行在协程中。
     */
    public static function isCoroutine(): bool
    {
        return \in_array(self::detect(), ['swoole', 'swow', 'fiber', 'coroutine'], true);
    }

    /**
     * 协程友好的休眠：不会阻塞其他协程。
     */
    public static function sleep(float $seconds): void
    {
        if ($seconds <= 0) {
            return;
        }

        switch (self::detect()) {
            case 'swoole':
            case 'coroutine':
                if (class_exists('\Swoole\Coroutine')) {
                    \Swoole\Coroutine::sleep($seconds);

                    return;
                }
                break;

            case 'swow':
                if (\function_exists('Swow\\Coroutine\\msleep')) {
                    \Swow\Coroutine\msleep((int) ($seconds * 1000));

                    return;
                }
                break;

            case 'fiber':
                // Fiber 场景交还调度权，由外层调度器决定何时恢复
                if (\Fiber::getCurrent() !== null) {
                    try {
                        \Fiber::suspend($seconds);

                        return;
                    } catch (\Throwable) {
                        // 无调度器接管时退化为阻塞休眠
                    }
                }
                break;
        }

        usleep((int) ($seconds * 1_000_000));
    }

    /**
     * 是否可以安装信号处理器（协程环境下由运行时统一接管）。
     */
    public static function supportsSignals(): bool
    {
        return \extension_loaded('pcntl') && !self::isCoroutine();
    }

    /**
     * 重置探测缓存（fork 之后必须调用）。
     */
    public static function reset(): void
    {
        self::$runtime = null;
    }

    private static function inSwooleCoroutine(): bool
    {
        return class_exists('\Swoole\Coroutine')
            && method_exists('\Swoole\Coroutine', 'getCid')
            && \Swoole\Coroutine::getCid() > 0;
    }
}
