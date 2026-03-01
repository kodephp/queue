# kode/queue

一个现代化、高性能的 PHP 队列客户端，支持多种队列驱动，提供统一的 API 接口，专为 PHP 8.1+ 设计。

## 核心特性

✅ 多驱动支持（Redis、Beanstalkd、AMQP 等）
✅ 自动驱动检测与配置
✅ 统一的队列操作 API
✅ 支持延迟入队功能
✅ 支持批量操作（批量入队、批量出队）
✅ 支持队列监控功能
✅ 完善的异常处理机制
✅ 支持事务功能
✅ 中间件支持（重试、超时、日志等）
✅ 符合 PSR 规范（PSR-1、PSR-2、PSR-4、PSR-12）
✅ PHP 8.1+ 兼容，支持 PHP 8.5+ 新特性

## 安装

使用 Composer 安装：

```bash
composer require kode/queue
```

## 驱动依赖

- **Redis**: `predis/predis`
- **Beanstalkd**: `pda/pheanstalk`
- **AMQP**: `php-amqplib/php-amqplib`
- **Kafka**: `ext-rdkafka` (PHP 扩展)

## 基本使用

### 创建队列实例

```php
use Kode\Queue\Factory;

// 创建队列实例
$queue = Factory::create([
    'default' => 'redis',
    'connections' => [
        'redis' => [
            'host' => '127.0.0.1',
            'port' => 6379,
            'database' => 0,
        ],
    ],
]);
```

### 基本操作

```php
// 入队
$jobId = $queue->push('SendEmail', ['user_id' => 123]);

// 延迟入队
$jobId = $queue->later(60, 'SendEmail', ['user_id' => 123]);

// 批量入队
$jobIds = $queue->bulk(['SendEmail', 'ProcessOrder'], ['user_id' => 123]);

// 出队
$job = $queue->pop();
if ($job) {
    // 处理任务
    echo $job['job']; // SendEmail
    echo $job['data']['user_id']; // 123
    
    // 删除任务
    $queue->delete($job['id']);
}

// 查看队列长度
$size = $queue->size();

// 查看队列统计信息
$stats = $queue->stats();
```

### 使用全局队列和局部队列

```php
// 获取全局队列实例（单例模式）
$globalQueue = $queue->global('global-queue');
$globalQueue->push('SendEmail', ['user_id' => 123]);

// 获取局部队列实例（每次创建新实例）
$localQueue = $queue->local('local-queue');
$localQueue->push('SendEmail', ['user_id' => 123]);

// 全局队列的高级用法
// 1. 在不同地方获取同一个全局队列实例
$queue1 = Factory::create()->global('orders');
$queue2 = Factory::create()->global('orders');
// $queue1 和 $queue2 是同一个实例

// 2. 全局队列的应用场景
// - 系统级任务队列
// - 跨模块共享的队列
// - 需要在多个地方访问的队列

// 局部队列的高级用法
// 1. 为不同任务创建独立的局部队列
$emailQueue = $queue->local('emails');
$orderQueue = $queue->local('orders');

// 2. 局部队列的应用场景
// - 临时任务队列
// - 单次操作的队列
// - 不需要跨模块共享的队列
```

### 使用中间件

```php
use Kode\Queue\Factory;
use Kode\Queue\Middleware\RetryMiddleware;
use Kode\Queue\Middleware\LogMiddleware;
use Kode\Queue\Middleware\RateLimitMiddleware;

// 创建队列实例
$queue = Factory::create();

// 添加中间件
$queue->addMiddleware(new RetryMiddleware(3, 100, 2.0))
      ->addMiddleware(new LogMiddleware())
      ->addMiddleware(new RateLimitMiddleware(10, 1));

// 使用队列
$jobId = $queue->push('SendEmail', ['user_id' => 123]);
```

### 使用事务

```php
use Kode\Queue\Factory;

$queue = Factory::create();

try {
    $queue->beginTransaction();
    
    // 执行多个队列操作
    $jobId1 = $queue->push('Task1', ['data' => 'value1']);
    $jobId2 = $queue->push('Task2', ['data' => 'value2']);
    
    $queue->commit();
} catch (\Exception $e) {
    $queue->rollback();
    // 处理异常
}
```

## 配置选项

### 通用配置

```php
$config = [
    'default' => 'redis', // 默认驱动
    'connections' => [
        'redis' => [
            'driver' => 'redis',
            'host' => '127.0.0.1',
            'port' => 6379,
            'database' => 0,
            'password' => null,
            'options' => [],
            'prefix' => 'queue:',
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
        'kafka' => [
            'driver' => 'kafka',
            'bootstrap_servers' => '127.0.0.1:9092',
            'topic' => 'queue',
            'group_id' => 'queue-consumer',
        ],
        'sync' => [
            'driver' => 'sync',
        ],
        'database' => [
            'driver' => 'database',
            'connection' => null,
            'table' => 'jobs',
        ],
    ],
];
```

## 支持的驱动

- **Redis**: 基于 Redis 的队列实现，支持延迟队列
- **Beanstalkd**: 基于 Beanstalkd 的队列实现，原生支持延迟队列
- **AMQP**: 基于 AMQP 协议的队列实现，支持 RabbitMQ 等消息队列系统
- **Sync**: 同步驱动，直接在内存中处理任务，适用于测试和开发环境
- **Database**: 基于数据库的队列实现，支持 MySQL、PostgreSQL 等数据库
- **Kafka**: 基于 Kafka 的队列实现，支持高吞吐量和持久化

## 异常处理

```php
use Kode\Queue\Exception\QueueException;
use Kode\Queue\Exception\DriverException;
use Kode\Queue\Exception\TransactionException;

try {
    $jobId = $queue->push('SendEmail', ['user_id' => 123]);
} catch (DriverException $e) {
    // 驱动异常
    echo "Driver error: " . $e->getMessage();
} catch (TransactionException $e) {
    // 事务异常
    echo "Transaction error: " . $e->getMessage();
} catch (QueueException $e) {
    // 队列异常
    echo "Queue error: " . $e->getMessage();
}
```

## 许可证

Apache-2.0
