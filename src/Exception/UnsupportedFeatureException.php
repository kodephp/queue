<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

use Kode\Queue\Enum\Capability;

/**
 * 驱动不支持某项能力时抛出。
 *
 * 例如对 Kafka 调用延迟投递、对 Redis 调用 bury。
 */
final class UnsupportedFeatureException extends QueueException
{
    public static function for(string $driver, Capability $capability): self
    {
        return new self(
            sprintf('驱动 [%s] 不支持「%s」能力', $driver, $capability->label()),
            context: ['driver' => $driver, 'capability' => $capability->value],
        );
    }
}
