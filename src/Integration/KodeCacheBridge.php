<?php

declare(strict_types=1);

namespace Kode\Queue\Integration;

use DateInterval;
use Override;
use Psr\SimpleCache\CacheInterface;

/**
 * kode/cache 桥接：把 `Kode\Cache\CacheManager` 适配成 PSR-16。
 *
 * 让 {@see \Kode\Queue\Middleware\IdempotencyMiddleware} 等组件
 * 只依赖 PSR-16 标准接口，而不与任何具体缓存实现耦合。
 */
final class KodeCacheBridge implements CacheInterface
{
    public function __construct(private readonly object $cache)
    {
        foreach (['get', 'set', 'delete', 'has'] as $method) {
            if (!method_exists($cache, $method)) {
                throw new \InvalidArgumentException(
                    sprintf('缓存实现 %s 缺少 %s() 方法', $cache::class, $method),
                );
            }
        }
    }

    /**
     * 自动探测 kode/cache；不可用时返回内存实现。
     */
    public static function detect(?object $cache = null): CacheInterface
    {
        if ($cache instanceof CacheInterface) {
            return $cache;
        }

        if ($cache !== null) {
            return new self($cache);
        }

        $class = 'Kode\\Cache\\CacheManager';
        if (class_exists($class) && method_exists($class, 'getInstance')) {
            /** @var object $manager */
            $manager = $class::getInstance();

            return new self($manager);
        }

        return new ArrayCache();
    }

    #[Override]
    public function get(string $key, mixed $default = null): mixed
    {
        return $this->cache->get($key, $default);
    }

    #[Override]
    public function set(string $key, mixed $value, null|int|DateInterval $ttl = null): bool
    {
        return (bool) $this->cache->set($key, $value, self::seconds($ttl));
    }

    #[Override]
    public function delete(string $key): bool
    {
        return (bool) $this->cache->delete($key);
    }

    #[Override]
    public function clear(): bool
    {
        return method_exists($this->cache, 'clear') ? (bool) $this->cache->clear() : false;
    }

    /**
     * @param  iterable<string>    $keys
     * @return iterable<string, mixed>
     */
    #[Override]
    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $out = [];
        foreach ($keys as $key) {
            $out[$key] = $this->get($key, $default);
        }

        return $out;
    }

    /**
     * @param iterable<string, mixed> $values
     */
    #[Override]
    public function setMultiple(iterable $values, null|int|DateInterval $ttl = null): bool
    {
        $ok = true;
        foreach ($values as $key => $value) {
            $ok = $this->set((string) $key, $value, $ttl) && $ok;
        }

        return $ok;
    }

    /**
     * @param iterable<string> $keys
     */
    #[Override]
    public function deleteMultiple(iterable $keys): bool
    {
        $ok = true;
        foreach ($keys as $key) {
            $ok = $this->delete($key) && $ok;
        }

        return $ok;
    }

    #[Override]
    public function has(string $key): bool
    {
        return (bool) $this->cache->has($key);
    }

    private static function seconds(null|int|DateInterval $ttl): ?int
    {
        return match (true) {
            $ttl === null => null,
            \is_int($ttl) => $ttl,
            default => (new \DateTimeImmutable('@0'))->add($ttl)->getTimestamp(),
        };
    }
}
