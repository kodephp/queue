<?php

namespace Kode\Queue;

use Kode\Queue\Driver\RedisDriver;
use Kode\Queue\Driver\BeanstalkdDriver;
use Kode\Queue\Driver\AmqpDriver;
use Kode\Queue\Driver\KafkaDriver;

class Factory {
    /**
     * 创建新的队列实例
     *
     * @param array $config 配置
     * @return QueueInterface 队列实例
     */
    public static function create(array $config = []): QueueInterface {
        $config = self::mergeConfig($config);
        $defaultDriver = $config['default'] ?? 'redis';
        $connectionConfig = $config['connections'][$defaultDriver] ?? [];

        return self::createWithDriver($defaultDriver, $connectionConfig);
    }

    /**
     * 使用特定驱动创建队列实例
     *
     * @param string $driver 驱动名称
     * @param array  $config 配置
     * @return QueueInterface 队列实例
     */
    public static function createWithDriver(string $driver, array $config = []): QueueInterface {
        $driverInstance = self::createDriver($driver, $config);
        $defaultQueue = $config['queue'] ?? $config['tube'] ?? 'default';

        return new class($driverInstance, $defaultQueue) extends AbstractQueue {
            // 继承 AbstractQueue 的匿名类
        };
    }

    /**
     * 创建驱动实例
     *
     * @param string $driver 驱动名称
     * @param array  $config 配置
     * @return Driver\DriverInterface 驱动实例
     */
    protected static function createDriver(string $driver, array $config = []): Driver\DriverInterface {
        switch (strtolower($driver)) {
            case 'redis':
                return new RedisDriver($config);
            case 'beanstalkd':
                return new BeanstalkdDriver($config);
            case 'amqp':
                return new AmqpDriver($config);
            case 'sync':
                return new Driver\SyncDriver($config);
            case 'database':
                return new Driver\DatabaseDriver($config);
            case 'kafka':
                return new KafkaDriver($config);
            default:
                throw new \InvalidArgumentException("不支持的驱动: {$driver}");
        }
    }

    /**
     * 合并给定配置与默认配置
     *
     * @param array $config 给定配置
     * @return array 合并后的配置
     */
    protected static function mergeConfig(array $config): array {
        return array_merge([
            'default' => 'redis',
            'connections' => [
                'redis' => [
                    'driver' => 'redis',
                    'host' => '127.0.0.1',
                    'port' => 6379,
                    'database' => 0,
                    'password' => null,
                    'options' => [],
                ],
                'beanstalkd' => [
                    'driver' => 'beanstalkd',
                    'host' => '127.0.0.1',
                    'port' => 11300,
                    'tube' => 'default',
                ],
                'amqp' => [
                    'driver' => 'amqp',
                    'host' => '127.0.0.1',
                    'port' => 5672,
                    'username' => 'guest',
                    'password' => 'guest',
                    'vhost' => '/',
                    'queue' => 'default',
                ],
                'sync' => [
                    'driver' => 'sync',
                ],
                'database' => [
                    'driver' => 'database',
                    'connection' => null,
                    'table' => 'jobs',
                ],
                'kafka' => [
                    'driver' => 'kafka',
                    'bootstrap_servers' => '127.0.0.1:9092',
                    'topic' => 'queue',
                    'group_id' => 'queue-consumer',
                ],
            ],
        ], $config);
    }
}