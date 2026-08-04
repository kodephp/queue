<?php

declare(strict_types=1);

namespace Kode\Queue;

use Kode\Queue\Config\ConnectionConfig;
use Kode\Queue\Config\QueueConfig;
use Kode\Queue\Contract\DriverInterface;
use Kode\Queue\Contract\EventDispatcherInterface;
use Kode\Queue\Contract\JobMiddlewareInterface;
use Kode\Queue\Contract\MiddlewareInterface;
use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Event\NullEventDispatcher;
use Kode\Queue\Integration\KodeEventBridge;
use Kode\Queue\Middleware\Pipeline;
use Kode\Queue\Serializer\SerializerFactory;

/**
 * 连接管理器 —— 整个包的入口。
 *
 * 取代 v1 的 `Factory`。两处关键差异：
 *
 * 1. **没有 switch**。驱动类、依赖要求、默认配置都长在 {@see DriverType} 上，
 *    新增一个驱动只需加一个枚举成员，管理器一行都不用改。
 * 2. **连接是复用的**。v1 每次 `Factory::create()` 都新建一条 Redis 连接，
 *    在长驻 Worker 里很快把连接数打满。这里按连接名缓存实例。
 *
 * 典型用法：
 *
 * ```php
 * $manager = QueueManager::make([
 *     'default' => 'redis',
 *     'connections' => [
 *         'redis' => ['driver' => 'redis', 'host' => '127.0.0.1', 'queue' => 'default'],
 *         'sync'  => ['driver' => 'sync'],
 *     ],
 * ]);
 *
 * $manager->push(SendMail::class, ['to' => 'a@b.c']);       // 走默认连接
 * $manager->connection('sync')->push(SendMail::class, []);  // 指定连接
 * ```
 */
final class QueueManager
{
    /** @var array<string, Queue> */
    private array $connections = [];

    /** @var array<string, DriverInterface> */
    private array $drivers = [];

    /** @var array<string, \Closure(ConnectionConfig, SerializerInterface): DriverInterface> */
    private array $extensions = [];

    /** @var list<MiddlewareInterface|JobMiddlewareInterface> 全局中间件 */
    private array $middleware = [];

    private EventDispatcherInterface $events;

    public function __construct(
        private readonly QueueConfig $config,
        ?EventDispatcherInterface $events = null,
        MiddlewareInterface ...$middleware,
    ) {
        // 未显式指定时自动探测 kode/event 或任意 PSR-14 派发器，都没有则退化为空派发器
        $this->events = $events ?? KodeEventBridge::detect();
        $this->middleware = $middleware;
    }

    /**
     * 从松散配置数组创建。
     *
     * @param array<string, mixed> $config
     */
    public static function make(array $config = [], ?EventDispatcherInterface $events = null): self
    {
        return new self(
            $config === [] ? QueueConfig::auto() : QueueConfig::fromArray($config),
            $events,
        );
    }

    /**
     * 从环境变量创建（12-factor 部署常用）。
     */
    public static function fromEnv(): self
    {
        return new self(QueueConfig::fromEnv());
    }

    /**
     * 零配置：自动挑选当前环境可用的最佳驱动（Redis → 数据库 → 内存）。
     */
    public static function auto(): self
    {
        return new self(QueueConfig::auto());
    }

    // ------------------------------------------------------------------
    // 连接
    // ------------------------------------------------------------------

    /**
     * 取得一条队列连接（同名连接复用同一实例）。
     */
    public function connection(?string $name = null): Queue
    {
        $name ??= $this->config->default;

        return $this->connections[$name] ??= $this->build($name);
    }

    /**
     * 默认连接的快捷别名。
     */
    public function default(): Queue
    {
        return $this->connection();
    }

    /**
     * 底层驱动实例。
     */
    public function driver(?string $name = null): DriverInterface
    {
        $name ??= $this->config->default;
        $this->connection($name);

        return $this->drivers[$name];
    }

    /**
     * 注册自定义驱动工厂，用于扩展枚举之外的后端。
     *
     * ```php
     * $manager->extend('nsq', fn (ConnectionConfig $c, SerializerInterface $s) => new NsqDriver($c, $s));
     * ```
     *
     * @param \Closure(ConnectionConfig, SerializerInterface): DriverInterface $factory
     */
    public function extend(string $driver, \Closure $factory): self
    {
        $this->extensions[strtolower($driver)] = $factory;
        // 已建好的同名连接需要失效，否则拿到的还是旧驱动
        $this->purge();

        return $this;
    }

    /**
     * 追加全局中间件（对所有连接生效，需在首次取连接前调用）。
     */
    public function pushMiddleware(MiddlewareInterface|JobMiddlewareInterface ...$middleware): self
    {
        $this->middleware = [...$this->middleware, ...$middleware];
        $this->connections = [];

        return $this;
    }

    public function withEvents(EventDispatcherInterface $events): self
    {
        $this->events = $events;
        $this->connections = [];

        return $this;
    }

    /**
     * 断开并丢弃连接缓存。
     */
    public function purge(?string $name = null): void
    {
        foreach ($name === null ? array_keys($this->drivers) : [$name] as $key) {
            ($this->drivers[$key] ?? null)?->close();
            unset($this->drivers[$key], $this->connections[$key]);
        }
    }

    /**
     * 所有已配置的连接名。
     *
     * @return list<string>
     */
    public function connectionNames(): array
    {
        return array_keys($this->config->connections);
    }

    public function config(): QueueConfig
    {
        return $this->config;
    }

    public function events(): EventDispatcherInterface
    {
        return $this->events;
    }

    /**
     * 各连接的健康状态诊断（配置脱敏后输出，可直接打日志）。
     *
     * @return array<string, array<string, mixed>>
     */
    public function diagnose(): array
    {
        $report = [];

        foreach ($this->config->connections as $name => $connection) {
            $report[$name] = [
                ...$connection->toSafeArray(),
                'available' => $connection->driver->isAvailable(),
                'install_hint' => $connection->driver->isAvailable() ? null : $connection->driver->installHint(),
                'capabilities' => array_map(
                    static fn (\BackedEnum $c): string => (string) $c->value,
                    $connection->driver->capabilities(),
                ),
                'connected' => isset($this->drivers[$name]) && $this->drivers[$name]->isConnected(),
            ];
        }

        return $report;
    }

    // ------------------------------------------------------------------
    // 默认连接代理（让 90% 的调用不必先写 ->connection()）
    // ------------------------------------------------------------------

    /**
     * @param array<string, mixed> $data
     */
    public function push(string|object $job, array $data = [], ?string $queue = null): string
    {
        return $this->connection()->push($job, $data, $queue);
    }

    /**
     * @param array<string, mixed> $data
     */
    public function later(
        int|\DateInterval|\DateTimeInterface $delay,
        string|object $job,
        array $data = [],
        ?string $queue = null,
    ): string {
        return $this->connection()->later($delay, $job, $data, $queue);
    }

    /**
     * @param  iterable<string|object|Message\Job> $jobs
     * @param  array<string, mixed>                $data
     * @return list<string>
     */
    public function bulk(iterable $jobs, array $data = [], ?string $queue = null): array
    {
        return $this->connection()->bulk($jobs, $data, $queue);
    }

    /**
     * @param array<string, mixed> $data
     */
    public function job(string|object $job, array $data = []): PendingDispatch
    {
        return $this->connection()->job($job, $data);
    }

    public function pop(?string $queue = null, float $timeout = 0.0): ?Message\ReservedJob
    {
        return $this->connection()->pop($queue, $timeout);
    }

    public function size(?string $queue = null): int
    {
        return $this->connection()->size($queue);
    }

    public function stats(?string $queue = null): Support\QueueStats
    {
        return $this->connection()->stats($queue);
    }

    /**
     * 关闭所有连接（进程退出前调用）。
     */
    public function close(): void
    {
        $this->purge();
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private function build(string $name): Queue
    {
        $connection = $this->config->connection($name);
        $serializer = SerializerFactory::make($connection->serializer);
        $driver = $this->makeDriver($connection, $serializer);

        $this->drivers[$name] = $driver;

        return new Queue(
            driver: $driver,
            connection: $name,
            defaultQueue: $connection->queue,
            pipeline: new Pipeline($this->middleware),
            events: $this->events,
        );
    }

    private function makeDriver(ConnectionConfig $connection, SerializerInterface $serializer): DriverInterface
    {
        $key = strtolower($connection->driver->value);

        if (isset($this->extensions[$key])) {
            return ($this->extensions[$key])($connection, $serializer);
        }

        $class = $connection->driver->driverClass();

        /** @var DriverInterface */
        return new $class($connection, $serializer);
    }
}
