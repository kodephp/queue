<?php

declare(strict_types=1);

namespace Kode\Queue\Driver\Redis;

use Override;

/**
 * Predis 适配器。
 *
 * 脚本一律先走 `EVALSHA`，只有 Redis 端确实没缓存（NOSCRIPT）才回退 `EVAL` 全文下发。
 * 这在高频 pop 场景下能省掉每次几 KB 的脚本传输。
 */
final class PredisClient implements RedisClientInterface
{
    /** @var array<string, true> 本进程已确认在服务端缓存过的脚本 sha */
    private array $loaded = [];

    public function __construct(private readonly object $client)
    {
    }

    #[Override]
    public function evaluate(string $script, array $keys = [], array $argv = []): mixed
    {
        $sha = sha1($script);
        $arguments = [\count($keys), ...$keys, ...$argv];

        if (isset($this->loaded[$sha])) {
            try {
                return $this->normalize($this->client->evalsha($sha, ...$arguments));
            } catch (\Throwable $e) {
                if (!$this->isNoScript($e)) {
                    throw $e;
                }
                unset($this->loaded[$sha]);
            }
        }

        $result = $this->client->eval($script, ...$arguments);
        $this->loaded[$sha] = true;

        return $this->normalize($result);
    }

    #[Override]
    public function zcard(string $key): int
    {
        return (int) $this->client->zcard($key);
    }

    #[Override]
    public function firstMember(string $key): ?string
    {
        $members = (array) $this->client->zrange($key, 0, 0);
        $first = reset($members);

        return \is_string($first) ? $first : null;
    }

    #[Override]
    public function llen(string $key): int
    {
        return (int) $this->client->llen($key);
    }

    #[Override]
    public function ping(): bool
    {
        try {
            $response = $this->client->ping();

            return $response === true
                || (\is_object($response) ? (string) $response : (string) $response) !== '';
        } catch (\Throwable) {
            return false;
        }
    }

    #[Override]
    public function disconnect(): void
    {
        if (method_exists($this->client, 'disconnect')) {
            $this->client->disconnect();
        }
    }

    #[Override]
    public function raw(): object
    {
        return $this->client;
    }

    /**
     * Predis 的空回复是 null，统一成 false 以便与 phpredis 保持一致。
     */
    private function normalize(mixed $value): mixed
    {
        return $value ?? false;
    }

    private function isNoScript(\Throwable $e): bool
    {
        return stripos($e->getMessage(), 'NOSCRIPT') !== false;
    }
}
