<?php

namespace Kode\Queue;

use Kode\Queue\Driver\DriverInterface;
use Kode\Queue\Middleware\MiddlewareInterface;

abstract class AbstractQueue implements QueueInterface {
    /**
     * 队列驱动
     *
     * @var DriverInterface
     */
    protected $driver;

    /**
     * 默认队列名称
     *
     * @var string
     */
    protected $defaultQueue;

    /**
     * 中间件栈
     *
     * @var array
     */
    protected $middleware = [];

    /**
     * 全局队列实例缓存
     *
     * @var array
     */
    protected static $globalInstances = [];

    /**
     * 创建新的队列实例
     *
     * @param DriverInterface $driver 驱动实例
     * @param string          $defaultQueue 默认队列名称
     */
    public function __construct(DriverInterface $driver, string $defaultQueue = 'default') {
        $this->driver = $driver;
        $this->defaultQueue = $defaultQueue;
    }

    /**
     * 添加中间件到栈
     *
     * @param MiddlewareInterface $middleware 中间件实例
     * @return $this
     */
    public function addMiddleware(MiddlewareInterface $middleware) {
        $this->middleware[] = $middleware;

        return $this;
    }

    /**
     * 管道操作符支持
     *
     * @param callable $callback 回调函数
     * @return mixed
     */
    public function pipe(callable $callback) {
        return $callback($this);
    }

    /**
     * 管道操作符重载
     *
     * @param callable $callback 回调函数
     * @return mixed
     */
    public function __invoke(callable $callback) {
        return $this->pipe($callback);
    }

    /**
     * 通过中间件栈调度队列操作
     *
     * @param string $method 方法名
     * @param array  $parameters 参数
     * @return mixed
     */
    protected function dispatch(string $method, array $parameters) {
        $stack = array_reverse($this->middleware);
        $next = function ($parameters) use ($method) {
            return $this->driver->{$method}(...$parameters);
        };

        foreach ($stack as $middleware) {
            $next = function ($parameters) use ($middleware, $next, $method) {
                return $middleware->handle($next, $method, $parameters);
            };
        }

        return $next($parameters);
    }

    /**
     * 推送任务到队列
     *
     * @param mixed  $job 任务名称或闭包
     * @param array  $data 任务数据
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return string 任务ID
     */
    public function push($job, array $data = [], string $queue = null): string {
        $payload = $this->createPayload($job, $data);

        return $this->dispatch('push', [$payload, $this->getQueue($queue)]);
    }

    /**
     * 推送原始负载到队列
     *
     * @param string $payload 原始负载
     * @param string $queue 队列名称，null 表示使用默认队列
     * @param array  $options 选项
     * @return string 任务ID
     */
    public function pushRaw(string $payload, string $queue = null, array $options = []): string {
        return $this->dispatch('push', [$payload, $this->getQueue($queue), $options]);
    }

    /**
     * 延迟推送任务到队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param mixed  $job 任务名称或闭包
     * @param array  $data 任务数据
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return string 任务ID
     */
    public function later(int $delay, $job, array $data = [], string $queue = null): string {
        $payload = $this->createPayload($job, $data);

        return $this->dispatch('later', [$delay, $payload, $this->getQueue($queue)]);
    }

    /**
     * 批量推送任务到队列
     *
     * @param array  $jobs 任务列表
     * @param array  $data 任务数据
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return array 任务ID列表
     */
    public function bulk(array $jobs, array $data = [], string $queue = null): array {
        $queue = $this->getQueue($queue);
        $jobIds = [];

        foreach ($jobs as $job) {
            $payload = $this->createPayload($job, $data);
            $jobIds[] = $this->dispatch('push', [$payload, $queue]);
        }

        return $jobIds;
    }

    /**
     * 从队列中取出下一个任务
     *
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return mixed 任务数据
     */
    public function pop(string $queue = null) {
        return $this->dispatch('pop', [$this->getQueue($queue)]);
    }

    /**
     * 获取队列大小
     *
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return int 队列大小
     */
    public function size(string $queue = null): int {
        return $this->dispatch('size', [$this->getQueue($queue)]);
    }

    /**
     * 从队列中删除任务
     *
     * @param string $jobId 任务ID
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return bool 是否删除成功
     */
    public function delete(string $jobId, string $queue = null): bool {
        return $this->dispatch('delete', [$jobId, $this->getQueue($queue)]);
    }

    /**
     * 将任务释放回队列
     *
     * @param int    $delay 延迟时间（秒）
     * @param string $jobId 任务ID
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return bool 是否释放成功
     */
    public function release(int $delay, string $jobId, string $queue = null): bool {
        return $this->dispatch('release', [$delay, $jobId, $this->getQueue($queue)]);
    }

    /**
     * 获取队列统计信息
     *
     * @param string $queue 队列名称，null 表示使用默认队列
     * @return array 统计信息
     */
    public function stats(string $queue = null): array {
        return $this->dispatch('stats', [$this->getQueue($queue)]);
    }

    /**
     * 开始事务
     *
     * @return void
     */
    public function beginTransaction(): void {
        $this->dispatch('beginTransaction', []);
    }

    /**
     * 提交事务
     *
     * @return void
     */
    public function commit(): void {
        $this->dispatch('commit', []);
    }

    /**
     * 回滚事务
     *
     * @return void
     */
    public function rollback(): void {
        $this->dispatch('rollback', []);
    }

    /**
     * 获取全局队列实例
     *
     * @param string $queue 队列名称
     * @return QueueInterface 队列实例
     */
    public function global(string $queue = 'default'): QueueInterface {
        $key = 'global_' . $queue;
        if (!isset(self::$globalInstances[$key])) {
            self::$globalInstances[$key] = new static($this->driver, $queue);
            foreach ($this->middleware as $middleware) {
                self::$globalInstances[$key]->addMiddleware($middleware);
            }
        }
        return self::$globalInstances[$key];
    }

    /**
     * 获取局部队列实例
     *
     * @param string $queue 队列名称
     * @return QueueInterface 队列实例
     */
    public function local(string $queue = 'default'): QueueInterface {
        $instance = new static($this->driver, $queue);
        foreach ($this->middleware as $middleware) {
            $instance->addMiddleware($middleware);
        }
        return $instance;
    }

    /**
     * 获取队列名称
     *
     * @param string $queue 队列名称
     * @return string
     */
    protected function getQueue(string $queue = null): string {
        return $queue ?? $this->defaultQueue;
    }

    /**
     * 从给定的任务和数据创建负载字符串
     *
     * @param mixed $job 任务名称或闭包
     * @param array $data 任务数据
     * @return string
     */
    protected function createPayload($job, array $data = []): string {
        return json_encode([
            'job' => $job,
            'data' => $data,
            'id' => uniqid('', true),
            'attempts' => 0,
            'created_at' => time(),
        ]);
    }

    /**
     * 获取驱动实例
     *
     * @return DriverInterface
     */
    public function getDriver(): DriverInterface {
        return $this->driver;
    }

    /**
     * 关闭连接
     *
     * @return void
     */
    public function close(): void {
        $this->driver->close();
    }
}
