<?php

declare(strict_types=1);

namespace Kode\Queue\Driver\Redis;

use Kode\Queue\Exception\DriverException;
use Override;

/**
 * ext-redis（phpredis）适配器。
 *
 * phpredis 的错误约定和 Predis 完全不同：它不抛异常，而是返回 `false`
 * 并把原因塞进 `getLastError()`。这里统一转换为异常，
 * 避免「脚本写错了却只看到 pop 一直返回 null」这种极难排查的静默失败。
 */
final class PhpRedisClient implements RedisClientInterface
{
    /** @var array<string, true> */
    private array $loaded = [];

    public function __construct(private readonly \Redis $client)
    {
    }

    #[Override]
    public function evaluate(string $script, array $keys = [], array $argv = []): mixed
    {
        $sha = sha1($script);
        $arguments = [...$keys, ...array_map(strval(...), $argv)];
        $keyCount = \count($keys);

        if (isset($this->loaded[$sha])) {
            $this->client->clearLastError();
            $result = $this->client->evalsha($sha, $arguments, $keyCount);
            $error = $this->client->getLastError();

            if ($error === null) {
                return $result;
            }

            if (stripos($error, 'NOSCRIPT') === false) {
                $this->client->clearLastError();

                throw DriverException::operationFailed('redis.evalsha', new \RuntimeException($error));
            }

            unset($this->loaded[$sha]);
        }

        $this->client->clearLastError();
        $result = $this->client->eval($script, $arguments, $keyCount);
        $error = $this->client->getLastError();

        if ($error !== null) {
            $this->client->clearLastError();

            throw DriverException::operationFailed('redis.eval', new \RuntimeException($error));
        }

        $this->loaded[$sha] = true;

        return $result;
    }

    #[Override]
    public function zcard(string $key): int
    {
        return (int) $this->client->zCard($key);
    }

    #[Override]
    public function firstMember(string $key): ?string
    {
        $members = $this->client->zRange($key, 0, 0);

        if (!\is_array($members) || $members === []) {
            return null;
        }

        $first = reset($members);

        return \is_string($first) ? $first : null;
    }

    #[Override]
    public function llen(string $key): int
    {
        return (int) $this->client->lLen($key);
    }

    #[Override]
    public function ping(): bool
    {
        try {
            return (bool) $this->client->ping();
        } catch (\Throwable) {
            return false;
        }
    }

    #[Override]
    public function disconnect(): void
    {
        try {
            $this->client->close();
        } catch (\Throwable) {
            // 连接本就已断开，忽略
        }
    }

    #[Override]
    public function raw(): object
    {
        return $this->client;
    }
}
