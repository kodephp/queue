# Kode\Queue

一个现代化、高性能的 PHP 队列客户端，支持多种队列驱动，专为 PHP 8.1+ 设计，完美支持 PHP 8.5 管道操作符新特性。

[![PHP Version](https://img.shields.io/badge/PHP-%3E%3D8.1-8892BF)](https://php.net)
[![License](https://img.shields.io/badge/License-Apache--2.0-green)](LICENSE)

## 目录

- [特性](#特性)
- [安装](#安装)
- [环境要求](#环境要求)
- [驱动依赖](#驱动依赖)
- [快速开始](#快速开始)
- [驱动配置](#驱动配置)
- [核心功能](#核心功能)
- [中间件](#中间件)
- [上下文管理](#上下文管理)
- [工具类](#工具类)
- [PHP 8.5 管道操作符](#php-85-管道操作符支持)
- [API 参考](#api-参考)
- [项目结构](#项目结构)
- [测试](#测试)

## 特性

- 多驱动支持（Redis、Beanstalkd、AMQP、Kafka、Database、Sync）
- 自动驱动检测与配置
- 统一的队列操作 API
- 支持延迟入队功能
- 支持批量操作（批量入队、批量出队）
- 支持队列监控功能
- 完善的异常处理机制
- 支持事务功能
- 中间件支持（重试、超时、日志等）
- 上下文管理（Context）
- 工具类支持（QueueUtil）
- 符合 PSR 规范（PSR-1、PSR-2、PSR-4、PSR-12）
- PHP 8.1+ 兼容，完美支持 PHP 8.5 管道操作符

## 安装

```bash
composer require kode/queue
```

## 环境要求

| 要求 | 版本 |
|------|------|
| PHP | >= 8.1 |
| ext-json | * |
| ext-pdo | * |

## 驱动依赖

| 驱动 | 依赖包 | 说明 | 安装命令 |
|------|--------|------|----------|
| Redis | `predis/predis` | Redis 驱动所需 | `composer require predis/predis` |
| Beanstalkd | `pda/pheanstalk` | Beanstalkd 驱动所需 | `composer require pda/pheanstalk` |
| AMQP | `php-amqplib/php-amqplib` | AMQP 驱动所需 | `composer require php-amqplib/php-amqplib` |
| Kafka | `ext-rdkafka` | PHP 扩展，Kafka 驱动所需 | `pecl install rdkafka` |
| Database | `ext-pdo` | 内置支持 | 内置 |
| Sync | 无 | 内置，用于测试 | 内置 |

## 快速开始

### 基本使用

```php
use Kode\Queue\Factory;

// 创建队列实例
$queue = Factory::create([
    'default' => 'redis',
    'connections' => [
        'redis' => [
            'host' => '127.0.0.1',
            'port' => 6379,
        ],
    ],
]);

// 入队
$jobId = $queue->push('SendEmail', ['user_id' => 123]);

// 延迟入队（60秒后执行）
$jobId = $queue->later(60, 'SendEmail', ['user_id' => 123]);

// 批量入队
$jobIds = $queue->bulk(['SendEmail', 'ProcessOrder'], ['user_id' => 123]);

// 出队
$job = $queue->pop();
if ($job) {
    // 处理任务
    echo $job['job']; // 任务名称
    print_r($job['data']); // 任务数据
}

// 查看队列长度
$size = $queue->size();

// 查看队列统计信息
$stats = $queue->stats();
```

### 使用指定驱动

```php
use Kode\Queue\Factory;

// 使用 Sync 驱动（同步执行，用于测试）
$queue = Factory::createWithDriver('sync');

// 使用 Redis 驱动
$queue = Factory::createWithDriver('redis', [
    'host' => '127.0.0.1',
    'port' => 6379,
    'database' => 0,
    'password' => null,
]);

// 使用 Database 驱动
$queue = Factory::createWithDriver('database', [
    'dsn' => 'mysql:host=127.0.0.1;dbname=queue',
    'username' => 'root',
    'password' => '',
    'table' => 'jobs',
]);

// 使用 Beanstalkd 驱动
$queue = Factory::createWithDriver('beanstalkd', [
    'host' => '127.0.0.1',
    'port' => 11300,
    'tube' => 'default',
]);

// 使用 AMQP 驱动
$queue = Factory::createWithDriver('amqp', [
    'host' => '127.0.0.1',
    'port' => 5672,
    'username' => 'guest',
    'password' => 'guest',
    'vhost' => '/',
    'queue' => 'default',
]);

// 使用 Kafka 驱动
$queue = Factory::createWithDriver('kafka', [
    'bootstrap_servers' => '127.0.0.1:9092',
    'topic' => 'queue',
    'group_id' => 'queue-consumer',
]);
```

## 驱动配置

### 完整配置示例

```php
$config = [
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
            'dsn' => 'mysql:host=127.0.0.1;dbname=queue',
            'username' => 'root',
            'password' => '',
            'table' => 'jobs',
        ],
    ],
];

$queue = Factory::create($config);
```

### 各驱动配置说明

#### Redis 驱动

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| host | string | 127.0.0.1 | Redis 服务器地址 |
| port | int | 6379 | Redis 端口 |
| database | int | 0 | 数据库编号 |
| password | string|null | null | 密码 |
| options | array | [] | Predis 选项 |

#### Database 驱动

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| dsn | string | - | PDO DSN 连接字符串 |
| username | string | null | 数据库用户名 |
| password | string | null | 数据库密码 |
| table | string | jobs | 任务表名 |

#### Beanstalkd 驱动

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| host | string | 127.0.0.1 | Beanstalkd 服务器地址 |
| port | int | 11300 | Beanstalkd 端口 |
| tube | string | default | 管道名称 |

#### AMQP 驱动

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| host | string | 127.0.0.1 | RabbitMQ 服务器地址 |
| port | int | 5672 | RabbitMQ 端口 |
| username | string | guest | 用户名 |
| password | string | guest | 密码 |
| vhost | string | / | 虚拟主机 |
| queue | string | default | 队列名称 |

#### Kafka 驱动

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| bootstrap_servers | string | 127.0.0.1:9092 | Kafka 服务器地址 |
| topic | string | queue | 主题名称 |
| group_id | string | queue-consumer | 消费者组ID |

## 核心功能

### 全局队列和局部队列

```php
use Kode\Queue\Factory;

$queue = Factory::createWithDriver('sync');

// 获取全局队列实例（单例模式）
$globalQueue1 = $queue->global('orders');
$globalQueue2 = $queue->global('orders');
// $globalQueue1 和 $globalQueue2 是同一个实例

// 全局队列应用场景：
// - 系统级任务队列
// - 跨模块共享的队列
// - 需要在多个地方访问的队列

// 获取局部队列实例（每次创建新实例）
$localQueue1 = $queue->local('emails');
$localQueue2 = $queue->local('emails');
// $localQueue1 和 $localQueue2 是不同实例

// 局部队列应用场景：
// - 临时任务队列
// - 单次操作的队列
// - 不需要跨模块共享的队列
```

### 使用事务

```php
use Kode\Queue\Factory;

$queue = Factory::createWithDriver('database');

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

### 延迟任务

```php
use Kode\Queue\Factory;

$queue = Factory::createWithDriver('sync');

// 延迟 60 秒执行
$jobId = $queue->later(60, 'SendEmail', ['user_id' => 123]);

// 延迟 5 分钟执行
$jobId = $queue->later(300, 'ProcessOrder', ['order_id' => 456]);

// 延迟 1 小时执行
$jobId = $queue->later(3600, 'CleanupTask', ['type' => 'logs']);
```

### 批量操作

```php
use Kode\Queue\Factory;

$queue = Factory::createWithDriver('sync');

// 批量推送任务
$jobIds = $queue->bulk([
    'SendEmail',
    'SendSms',
    'SendPush',
], ['user_id' => 123]);

// 批量推送不同数据的任务
$jobs = [
    ['job' => 'SendEmail', 'data' => ['user_id' => 1]],
    ['job' => 'SendEmail', 'data' => ['user_id' => 2]],
    ['job' => 'SendEmail', 'data' => ['user_id' => 3]],
];

foreach ($jobs as $job) {
    $queue->push($job['job'], $job['data']);
}
```

## 中间件

### 内置中间件

| 中间件 | 说明 | 参数 |
|--------|------|------|
| `LogMiddleware` | 记录队列操作日志 | `callable $logger` (可选) |
| `RetryMiddleware` | 自动重试失败的任务 | `int $maxAttempts`, `int $delay`, `float $multiplier` |
| `RateLimitMiddleware` | 限制队列操作速率 | `int $capacity`, `float $rate` |

### 使用中间件

```php
use Kode\Queue\Factory;
use Kode\Queue\Middleware\RetryMiddleware;
use Kode\Queue\Middleware\LogMiddleware;
use Kode\Queue\Middleware\RateLimitMiddleware;

$queue = Factory::createWithDriver('sync');

// 添加中间件
$queue->addMiddleware(new RetryMiddleware(3, 100, 2.0))
      ->addMiddleware(new LogMiddleware())
      ->addMiddleware(new RateLimitMiddleware(10, 1));

// 使用队列
$jobId = $queue->push('SendEmail', ['user_id' => 123]);
```

### 自定义中间件

```php
use Kode\Queue\Middleware\MiddlewareInterface;

class CustomMiddleware implements MiddlewareInterface {
    public function handle(callable $next, string $method, array $parameters) {
        // 前置处理
        echo "Before: $method\n";
        
        // 执行下一个处理器
        $result = $next($parameters);
        
        // 后置处理
        echo "After: $method\n";
        
        return $result;
    }
}

// 使用自定义中间件
$queue->addMiddleware(new CustomMiddleware());
```

## 上下文管理

`Context` 类用于封装和管理队列任务的上下文信息，提供便捷的方法来访问和操作任务数据。

### 创建上下文

```php
use Kode\Queue\Context\Context;

// 从任务数据创建上下文
$context = new Context([
    'id' => 'job_123',
    'job' => 'SendEmail',
    'data' => ['user_id' => 123],
    'attempts' => 0,
    'created_at' => time(),
]);

// 从数组创建
$context = Context::fromArray($jobData);

// 从 JSON 创建
$context = Context::fromJson('{"id":"job_123","job":"SendEmail",...}');
```

### 获取任务信息

```php
// 获取任务ID
$jobId = $context->getJobId();        // 'job_123'

// 获取任务名称
$jobName = $context->getJob();        // 'SendEmail'

// 获取任务数据
$jobData = $context->getData();       // ['user_id' => 123]

// 获取重试次数
$attempts = $context->getAttempts();  // 0

// 获取创建时间
$createdAt = $context->getCreatedAt(); // 时间戳

// 获取队列名称
$queue = $context->getQueue();        // 队列名称

// 获取完整负载
$payload = $context->getPayload();    // 完整数组数据
```

### 操作上下文

```php
// 增加重试次数
$context->incrementAttempts();

// 转换为数组
$payload = $context->toArray();

// 转换为 JSON
$json = $context->toJson();
```

### 在任务处理器中使用

```php
use Kode\Queue\Context\Context;

class SendEmailHandler {
    public function handle(array $jobData) {
        $context = new Context($jobData);
        
        // 获取任务信息
        $userId = $context->getData()['user_id'];
        $attempts = $context->getAttempts();
        
        // 执行任务逻辑
        try {
            $this->sendEmail($userId);
        } catch (\Exception $e) {
            // 记录重试
            $context->incrementAttempts();
            
            // 判断是否超过最大重试次数
            if ($context->getAttempts() >= 3) {
                throw $e;
            }
            
            // 重新入队
            return false;
        }
        
        return true;
    }
}
```

## 工具类

`QueueUtil` 类提供队列操作中常用的工具方法。

### 生成任务ID

```php
use Kode\Queue\Util\QueueUtil;

// 生成唯一任务ID
$jobId = QueueUtil::generateJobId();
// 输出: 67e8f12345678.12345678
```

### 创建和解析负载

```php
use Kode\Queue\Util\QueueUtil;

// 创建任务负载
$payload = QueueUtil::createPayload('SendEmail', ['user_id' => 123]);
// 输出: {"job":"SendEmail","data":{"user_id":123},"id":"...","attempts":0,"created_at":1234567890}

// 解析任务负载
$data = QueueUtil::parsePayload($payload);
// 输出: ['job' => 'SendEmail', 'data' => ['user_id' => 123], ...]
```

### 检查任务状态

```php
use Kode\Queue\Util\QueueUtil;

// 检查任务是否就绪（用于延迟任务）
$job = ['available_at' => time() - 10];
$isReady = QueueUtil::isJobReady($job);  // true

$job = ['available_at' => time() + 60];
$isReady = QueueUtil::isJobReady($job);  // false
```

### 计算延迟

```php
use Kode\Queue\Util\QueueUtil;

// 计算延迟后的时间戳
$timestamp = QueueUtil::calculateDelay(60);  // 当前时间 + 60秒
```

### 格式化队列大小

```php
use Kode\Queue\Util\QueueUtil;

// 格式化队列大小
echo QueueUtil::formatSize(500);     // "500"
echo QueueUtil::formatSize(1500);    // "1.50K"
echo QueueUtil::formatSize(1500000); // "1.50M"
```

### 验证名称

```php
use Kode\Queue\Util\QueueUtil;

// 验证队列名称
QueueUtil::validateQueueName('my-queue');    // true
QueueUtil::validateQueueName('my_queue');    // true
QueueUtil::validateQueueName('my.queue');    // true
QueueUtil::validateQueueName('my queue');    // false (包含空格)

// 验证任务名称
QueueUtil::validateJobName('SendEmail');         // true
QueueUtil::validateJobName('App\Jobs\SendEmail'); // true
QueueUtil::validateJobName('123SendEmail');      // false (数字开头)
```

### 指数退避

```php
use Kode\Queue\Util\QueueUtil;

// 计算指数退避延迟时间（毫秒）
QueueUtil::exponentialBackoff(1, 100, 2.0);  // 100ms (第1次重试)
QueueUtil::exponentialBackoff(2, 100, 2.0);  // 200ms (第2次重试)
QueueUtil::exponentialBackoff(3, 100, 2.0);  // 400ms (第3次重试)
QueueUtil::exponentialBackoff(4, 100, 2.0);  // 800ms (第4次重试)

// 自定义参数
QueueUtil::exponentialBackoff(3, 50, 1.5);   // 112ms (基础延迟50ms，乘数1.5)
```

## PHP 8.5 管道操作符支持

PHP 8.5 引入了管道操作符 `|>`，本包完美支持这一新特性。

### 传统写法

```php
use Kode\Queue\Factory;
use Kode\Queue\Middleware\LogMiddleware;

$queue = Factory::createWithDriver('sync');
$queue->addMiddleware(new LogMiddleware());
$jobId = $queue->push('SendEmail', ['user_id' => 123]);
```

### PHP 8.5 管道操作符写法

```php
use Kode\Queue\Factory;
use Kode\Queue\Middleware\LogMiddleware;
use Kode\Queue\Middleware\RetryMiddleware;

// 基本管道操作
$jobId = Factory::createWithDriver('sync')
    |> fn($q) => $q->push('SendEmail', ['user_id' => 123]);

// 管道操作与中间件
$jobId = Factory::createWithDriver('sync')
    |> fn($q) => $q->addMiddleware(new LogMiddleware())
    |> fn($q) => $q->addMiddleware(new RetryMiddleware(3, 100, 2.0))
    |> fn($q) => $q->push('SendEmail', ['user_id' => 123]);

// 管道操作与全局队列
$jobId = Factory::createWithDriver('sync')
    |> fn($q) => $q->global('emails')
    |> fn($q) => $q->push('SendEmail', ['user_id' => 123]);

// 管道操作与延迟任务
$jobId = Factory::createWithDriver('sync')
    |> fn($q) => $q->later(60, 'DelayedJob', ['data' => 'delayed']);

// 管道操作与批量任务
$jobIds = Factory::createWithDriver('sync')
    |> fn($q) => $q->bulk(['Job1', 'Job2', 'Job3'], ['data' => 'bulk']);
```

### 兼容 PHP 8.1-8.4 的管道方法

对于 PHP 8.5 以下版本，可以使用 `pipe()` 方法实现类似功能：

```php
use Kode\Queue\Factory;
use Kode\Queue\Middleware\LogMiddleware;

// 使用 pipe 方法
$jobId = Factory::createWithDriver('sync')
    ->pipe(fn($q) => $q->addMiddleware(new LogMiddleware()))
    ->pipe(fn($q) => $q->push('SendEmail', ['user_id' => 123]));

// 链式 pipe 调用
$result = Factory::createWithDriver('sync')
    ->pipe(fn($q) => $q->addMiddleware(new LogMiddleware()))
    ->pipe(fn($q) => $q->push('Task1', ['data' => 'value1']))
    ->pipe(fn($q) => $q->push('Task2', ['data' => 'value2']))
    ->pipe(fn($q) => $q->size());
```

## API 参考

### QueueInterface

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `push` | `$job, array $data = [], string $queue = null` | `string` | 推送任务到队列 |
| `pushRaw` | `string $payload, string $queue = null, array $options = []` | `string` | 推送原始负载到队列 |
| `later` | `int $delay, $job, array $data = [], string $queue = null` | `string` | 延迟推送任务到队列 |
| `bulk` | `array $jobs, array $data = [], string $queue = null` | `array` | 批量推送任务到队列 |
| `pop` | `string $queue = null` | `mixed` | 从队列中取出下一个任务 |
| `size` | `string $queue = null` | `int` | 获取队列大小 |
| `delete` | `string $jobId, string $queue = null` | `bool` | 从队列中删除任务 |
| `release` | `int $delay, string $jobId, string $queue = null` | `bool` | 将任务释放回队列 |
| `stats` | `string $queue = null` | `array` | 获取队列统计信息 |
| `beginTransaction` | - | `void` | 开始事务 |
| `commit` | - | `void` | 提交事务 |
| `rollback` | - | `void` | 回滚事务 |
| `global` | `string $queue = 'default'` | `QueueInterface` | 获取全局队列实例 |
| `local` | `string $queue = 'default'` | `QueueInterface` | 获取局部队列实例 |
| `pipe` | `callable $callback` | `mixed` | 管道操作 |
| `addMiddleware` | `MiddlewareInterface $middleware` | `$this` | 添加中间件 |

## 项目结构

```
src/
├── Driver/                  # 驱动目录
│   ├── DriverInterface.php  # 驱动接口
│   ├── SyncDriver.php       # 同步驱动
│   ├── DatabaseDriver.php   # 数据库驱动
│   ├── RedisDriver.php      # Redis 驱动
│   ├── BeanstalkdDriver.php # Beanstalkd 驱动
│   ├── AmqpDriver.php       # AMQP 驱动
│   └── KafkaDriver.php      # Kafka 驱动
├── Exception/               # 异常目录
│   ├── QueueException.php   # 队列异常基类
│   ├── DriverException.php  # 驱动异常
│   └── TransactionException.php # 事务异常
├── Middleware/              # 中间件目录
│   ├── MiddlewareInterface.php # 中间件接口
│   ├── LogMiddleware.php    # 日志中间件
│   ├── RetryMiddleware.php  # 重试中间件
│   └── RateLimitMiddleware.php # 限流中间件
├── Context/                 # 上下文目录
│   └── Context.php          # 队列操作上下文
├── Util/                    # 工具目录
│   └── QueueUtil.php        # 队列工具类
├── AbstractQueue.php        # 抽象队列基类
├── Factory.php              # 工厂类
└── QueueInterface.php       # 队列接口
```

## 测试

```bash
# 运行测试
composer test

# 生成测试覆盖率报告
composer test-coverage
```