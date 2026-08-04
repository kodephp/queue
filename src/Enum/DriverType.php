<?php

declare(strict_types=1);

namespace Kode\Queue\Enum;

use Kode\Queue\Contract\DriverInterface;
use Kode\Queue\Driver\AmqpDriver;
use Kode\Queue\Driver\BeanstalkdDriver;
use Kode\Queue\Driver\DatabaseDriver;
use Kode\Queue\Driver\KafkaDriver;
use Kode\Queue\Driver\MemoryDriver;
use Kode\Queue\Driver\NullDriver;
use Kode\Queue\Driver\RedisDriver;
use Kode\Queue\Driver\SyncDriver;

/**
 * 队列驱动类型枚举。
 *
 * 以枚举取代旧版的字符串 switch 分支，获得编译期可枚举性、IDE 补全与穷尽性检查。
 * 每个成员同时携带「实现类」「依赖要求」「默认配置」三类元信息，
 * 使 {@see \Kode\Queue\QueueManager} 无需再维护任何硬编码映射表。
 */
enum DriverType: string
{
    case Redis = 'redis';
    case Database = 'database';
    case Beanstalkd = 'beanstalkd';
    case Amqp = 'amqp';
    case Kafka = 'kafka';
    case Memory = 'memory';
    case Sync = 'sync';
    case Null = 'null';

    /** 默认驱动（未显式配置时使用） */
    public const self DEFAULT = self::Memory;

    /**
     * 宽松解析：不区分大小写，并兼容常见别名。
     *
     * @throws \ValueError 当名称无法识别时
     */
    public static function fromName(string $name): self
    {
        return self::tryFromName($name)
            ?? throw new \ValueError(sprintf(
                '不支持的队列驱动 "%s"，可用值：%s',
                $name,
                implode('、', array_column(self::cases(), 'value')),
            ));
    }

    /**
     * 宽松解析，失败返回 null。
     */
    public static function tryFromName(string $name): ?self
    {
        $normalized = strtolower(trim($name));

        return match ($normalized) {
            'rabbitmq', 'rabbit' => self::Amqp,
            'db', 'mysql', 'pgsql', 'sqlite', 'pdo' => self::Database,
            'array', 'in-memory', 'inmemory' => self::Memory,
            'beanstalk' => self::Beanstalkd,
            'none', 'void', 'blackhole' => self::Null,
            default => self::tryFrom($normalized),
        };
    }

    /**
     * 驱动实现类的完全限定名。
     *
     * @return class-string<DriverInterface>
     */
    public function driverClass(): string
    {
        return match ($this) {
            self::Redis => RedisDriver::class,
            self::Database => DatabaseDriver::class,
            self::Beanstalkd => BeanstalkdDriver::class,
            self::Amqp => AmqpDriver::class,
            self::Kafka => KafkaDriver::class,
            self::Memory => MemoryDriver::class,
            self::Sync => SyncDriver::class,
            self::Null => NullDriver::class,
        };
    }

    /**
     * 运行该驱动所需的类 / 扩展（用于可用性自检与友好报错）。
     *
     * @return list<string> 形如 ['class:Predis\Client', 'ext:pdo']
     */
    public function requirements(): array
    {
        return match ($this) {
            self::Redis => ['class:Predis\Client|ext:redis'],
            self::Database => ['ext:pdo'],
            self::Beanstalkd => ['class:Pheanstalk\Pheanstalk'],
            self::Amqp => ['class:PhpAmqpLib\Connection\AMQPStreamConnection'],
            self::Kafka => ['ext:rdkafka'],
            self::Memory, self::Sync, self::Null => [],
        };
    }

    /**
     * 安装建议（依赖缺失时给用户看的提示）。
     */
    public function installHint(): string
    {
        return match ($this) {
            self::Redis => 'composer require predis/predis 或 pecl install redis',
            self::Database => '启用 PHP 内置 pdo 扩展',
            self::Beanstalkd => 'composer require pda/pheanstalk',
            self::Amqp => 'composer require php-amqplib/php-amqplib',
            self::Kafka => 'pecl install rdkafka',
            self::Memory, self::Sync, self::Null => '无需额外依赖',
        };
    }

    /**
     * 当前环境是否具备运行该驱动的条件。
     */
    public function isAvailable(): bool
    {
        foreach ($this->requirements() as $requirement) {
            // "|" 表示多选一（例如 Redis 客户端二选一）
            $satisfied = false;
            foreach (explode('|', $requirement) as $candidate) {
                [$kind, $target] = explode(':', $candidate, 2);
                $satisfied = match ($kind) {
                    'class' => class_exists($target),
                    'ext' => \extension_loaded($target),
                    default => false,
                };
                if ($satisfied) {
                    break;
                }
            }

            if (!$satisfied) {
                return false;
            }
        }

        return true;
    }

    /**
     * 该驱动的默认连接配置。
     *
     * @return array<string, mixed>
     */
    public function defaultConfig(): array
    {
        return match ($this) {
            self::Redis => [
                'scheme' => 'tcp',
                'host' => '127.0.0.1',
                'port' => 6379,
                'database' => 0,
                'password' => null,
                'prefix' => 'kode:queue:',
                'client' => 'auto', // auto | predis | phpredis
                'block_timeout' => 5,
                'retry_after' => 90,
                'options' => [],
            ],
            self::Database => [
                'dsn' => 'sqlite::memory:',
                'username' => null,
                'password' => null,
                'table' => 'jobs',
                'failed_table' => 'failed_jobs',
                'retry_after' => 90,
                'auto_migrate' => true,
            ],
            self::Beanstalkd => [
                'host' => '127.0.0.1',
                'port' => 11300,
                'timeout' => 10,
                'tube' => 'default',
                'retry_after' => 90,
            ],
            self::Amqp => [
                'host' => '127.0.0.1',
                'port' => 5672,
                'username' => 'guest',
                'password' => 'guest',
                'vhost' => '/',
                'queue' => 'default',
                'exchange' => '',
                'durable' => true,
                'heartbeat' => 30,
            ],
            self::Kafka => [
                'bootstrap_servers' => '127.0.0.1:9092',
                'topic' => 'queue',
                'group_id' => 'kode-queue',
                'auto_offset_reset' => 'earliest',
                'consume_timeout' => 1000,
                'conf' => [],
            ],
            self::Memory, self::Sync, self::Null => [],
        };
    }

    /**
     * 该驱动天然支持的能力集合。
     *
     * @return list<Capability>
     */
    public function capabilities(): array
    {
        return match ($this) {
            self::Redis => [
                Capability::Delay, Capability::Batch, Capability::Ack,
                Capability::Release, Capability::Clear, Capability::Size,
                Capability::Blocking, Capability::Transaction, Capability::Peek,
                // 就绪队列基于 ZSET，分值携带优先级偏移
                Capability::Priority, Capability::Bury,
            ],
            self::Database => [
                Capability::Delay, Capability::Batch, Capability::Ack,
                Capability::Release, Capability::Clear, Capability::Size,
                Capability::Transaction, Capability::Priority, Capability::Peek,
            ],
            self::Beanstalkd => [
                Capability::Delay, Capability::Ack, Capability::Release,
                Capability::Size, Capability::Priority, Capability::Bury,
                Capability::Blocking, Capability::Peek,
            ],
            self::Amqp => [
                Capability::Ack, Capability::Release, Capability::Size,
                Capability::Priority, Capability::Blocking, Capability::Clear,
            ],
            self::Kafka => [Capability::Ack, Capability::Batch, Capability::Blocking],
            self::Memory, self::Sync => [
                Capability::Delay, Capability::Batch, Capability::Ack,
                Capability::Release, Capability::Clear, Capability::Size,
                Capability::Priority, Capability::Peek,
            ],
            self::Null => [Capability::Ack, Capability::Clear, Capability::Size],
        };
    }

    /**
     * 是否为进程内驱动（不跨进程、不持久化）。
     */
    public function isInProcess(): bool
    {
        return match ($this) {
            self::Memory, self::Sync, self::Null => true,
            default => false,
        };
    }

    /**
     * 人类可读的中文标签。
     */
    public function label(): string
    {
        return match ($this) {
            self::Redis => 'Redis',
            self::Database => '关系型数据库',
            self::Beanstalkd => 'Beanstalkd',
            self::Amqp => 'AMQP / RabbitMQ',
            self::Kafka => 'Apache Kafka',
            self::Memory => '进程内内存队列',
            self::Sync => '同步执行',
            self::Null => '空驱动（丢弃）',
        };
    }
}
