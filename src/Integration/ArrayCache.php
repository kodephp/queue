<?php

declare(strict_types=1);

namespace Kode\Queue\Integration;

use DateInterval;
use Override;
use Psr\SimpleCache\CacheInterface;

/**
 * 极简进程内 PSR-16 缓存：没有任何缓存实现可用时的兜底。
 *
 * 仅在单进程内有效，不做持久化——用于测试与单机降级。
 */
final class ArrayCache implements CacheInterface
{
    /** @var array<string, array{value: mixed, expiresAt: float}> */
    private array $items = [];

    public function __construct(private readonly int $maxItems = 10_000)
    {
    }

    #[Override]
    public function get(string $key, mixed $default = null): mixed
    {
        $item = $this->items[$key] ?? null;

        if ($item === null) {
            return $default;
        }

        if ($item['expiresAt'] > 0 && $item['expiresAt'] < microtime(true)) {
            unset($this->items[$key]);

            return $default;
        }

        return $item['value'];
    }

    #[Override]
    public function set(string $key, mixed $value, null|int|DateInterval $ttl = null): bool
    {
        // 简单的容量保护：超限时淘汰最早写入的 10%
        if (\count($this->items) >= $this->maxItems) {
            $this->items = \array_slice($this->items, (int) ($this->maxItems * 0.1), preserve_keys: true);
        }

        $seconds = match (true) {
            $ttl === null => 0,
            \is_int($ttl) => $ttl,
            default => (new \DateTimeImmutable('@0'))->add($ttl)->getTimestamp(),
        };

        $this->items[$key] = [
            'value' => $value,
            'expiresAt' => $seconds > 0 ? microtime(true) + $seconds : 0.0,
        ];

        return true;
    }

    #[Override]
    public function delete(string $key): bool
    {
        unset($this->items[$key]);

        return true;
    }

    #[Override]
    public function clear(): bool
    {
        $this->items = [];

        return true;
    }

    #[Override]
    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $out = [];
        foreach ($keys as $key) {
            $out[$key] = $this->get($key, $default);
        }

        return $out;
    }

    #[Override]
    public function setMultiple(iterable $values, null|int|DateInterval $ttl = null): bool
    {
        foreach ($values as $key => $value) {
            $this->set((string) $key, $value, $ttl);
        }

        return true;
    }

    #[Override]
    public function deleteMultiple(iterable $keys): bool
    {
        foreach ($keys as $key) {
            $this->delete($key);
        }

        return true;
    }

    #[Override]
    public function has(string $key): bool
    {
        return $this->get($key, $this) !== $this;
    }
}
