# Kode\Queue

现代化 PHP 8.3+ 队列组件。不可变消息对象、可见性超时与至少一次投递、内建 Worker 与死信存储，
一套 API 覆盖 Redis / 数据库 / Beanstalkd / AMQP / Kafka / 内存 / 同步。

[![PHP Version](https://img.shields.io/badge/php-%3E%3D8.3-777bb4.svg)](https://php.net)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-2.0.0-brightgreen.svg)](https://github.com/kodephp/queue/releases)

```php
$queue = QueueManager::auto()->default();

$queue->push('mail.send', ['to' => 'a@example.com']);          // 投递
Worker::for($queue, ['mail.send' => $sendMail])->run();        // 消费
```

---

## 目录

- [v2 做了什么](#v2-做了什么)
- [特性](#特性)
- [环境要求与安装](#环境要求与安装)
- [60 秒上手](#60-秒上手)
- [核心概念](#核心概念)
- [投递任务](#投递任务)
- [消费任务](#消费任务)
- [配置](#配置)
- [驱动能力矩阵](#驱动能力矩阵)
- [失败、重试与死信](#失败重试与死信)
- [中间件](#中间件)
- [事件](#事件)
- [命令行工具](#命令行工具)
- [与 kode 生态集成](#与-kode-生态集成)
- [序列化](#序列化)
- [协程与 Swoole](#协程与-swoole)
- [测试](#测试)
- [v1 → v2 迁移指南](#v1--v2-迁移指南)
- [目录结构](#目录结构)

---

## v2 做了什么

v1 是一个「能把数据塞进 Redis 再取出来」的队列，v2 是一个**能在生产环境跑住的队列**。

| 维度 | v1.x | v2.0 |
| --- | --- | --- |
| 最低 PHP | 8.1 | **8.3**（枚举类型化常量、`#[Override]`、`json_validate`、`Randomizer`） |
| 消息载体 | 裸数组 `['job' => ..., 'data' => ...]` | `readonly class Job` / `ReservedJob`（仍兼容数组下标读取） |
| 任务 ID | `uniqid()` | **ULID**，同毫秒单调自增，字典序即时间序 |
| 投递语义 | pop 即删除，进程崩溃任务就没了 | **至少一次**：pop → ack / release / fail，带可见性超时与自动回收 |
| 消费端 | 自己写 `while (true)` | 内建 `Worker`：信号优雅退出、超时熔断、内存红线、自愈重启 |
| 失败处理 | 无 | 死信存储三选一（内存 / 文件 / 数据库）+ CLI 重放 |
| 参数声明 | 每个投递点手写数组 | `#[AsJob]` 属性绑在任务类上，投递方只管投 |
| 命令行 | 无 | `vendor/bin/kode-queue`，零第三方依赖 |
| 驱动 | 5 个 | 8 个（新增 memory / null，补齐 sync），能力可编程探测 |

## 特性

- **不可变消息对象** —— `Job` 与 `ReservedJob` 都是 `readonly class`，任何变更都通过 `withXxx()` 产生新实例，
  杜绝「中间件偷偷改了 payload 导致重试时行为不一致」这类脏问题。
- **至少一次投递** —— `pop()` 返回带回执的 `ReservedJob`，处理完 `ack()`，失败 `release()` 重投或 `fail()` 落死信。
  Worker 崩溃后，超过可见性超时的预留任务会被 `reclaimExpired()` 自动捞回。
- **优先级与延迟** —— 五档 `Priority` 枚举，各驱动自动映射到原生机制（Redis ZSet 打分 / Beanstalkd 1024 基准 / AMQP 0-10）。
- **五种退避策略** —— 固定、线性、指数、**指数 + 抖动**（默认）、斐波那契，避免重试风暴同时打爆下游。
- **能力探测** —— `$queue->supports(Capability::Delay)`，不同后端能力差异在代码里可判断，而不是运行时炸给你看。
- **零依赖 CLI** —— 不引入 symfony/console，`work` / `stats` / `diagnose` / `failed:*` 共 9 个命令。
- **软集成 kode 生态** —— kode/event、kode/context、kode/di、kode/cache、kode/limiting 装了就自动接上，没装也照常跑。
- **协程友好** —— 自动识别 Swoole / Swow / Fiber，休眠时让出调度权而不是阻塞整个 Worker。

## 环境要求与安装

```bash
composer require kode/queue
```

| 项目 | 要求 |
| --- | --- |
| PHP | >= 8.3 |
| 必需扩展 | `ext-json`、`ext-mbstring` |
| 强烈建议 | `ext-pcntl`（Worker 优雅退出与超时中断） |

按需安装驱动依赖：

| 驱动 | 依赖 | 安装 |
| --- | --- | --- |
| Redis | `ext-redis`（首选）或 Predis | `pecl install redis` / `composer require predis/predis` |
| Database | `ext-pdo` | 内置 |
| Beanstalkd | Pheanstalk 4.x / 5.x | `composer require pda/pheanstalk` |
| AMQP | php-amqplib | `composer require php-amqplib/php-amqplib` |
| Kafka | `ext-rdkafka` | `pecl install rdkafka` |
| Memory / Sync / Null | 无 | 内置 |

不确定当前环境能跑哪个？直接问它：

```bash
vendor/bin/kode-queue diagnose --bootstrap=queue.php
# ✓ redis    driver=redis    能力：delay, priority, batch, ack, release, bury, clear, size, blocking, transaction, peek
# ✗ kafka    driver=kafka    请安装 ext-rdkafka
```

## 60 秒上手

```php
use Kode\Queue\QueueManager;
use Kode\Queue\Worker;

$manager = QueueManager::make([
    'default' => 'redis',
    'connections' => [
        'redis' => ['driver' => 'redis', 'host' => '127.0.0.1', 'port' => 6379],
    ],
]);

$queue = $manager->default();

// ---------- 生产端 ----------
$queue->push('mail.send', ['to' => 'a@example.com']);           // 立即
$queue->later(60, 'report.build', ['month' => '2026-07']);      // 60 秒后
$queue->job('order.settle', ['id' => 9527])                     // 流式声明
      ->onQueue('high')
      ->retries(5)
      ->timeout(120)
      ->dispatch();

// ---------- 消费端 ----------
Worker::for($queue, [
    'mail.send'    => fn (array $p) => Mailer::send($p['to']),
    'report.build' => ReportBuilder::class,   // 类名：自动解析 handle() / __invoke() / run()
    'order.settle' => $settleHandler,
])->run();
```

不想写配置？`QueueManager::auto()` 会按 Redis → Database → Memory 挑第一个可用的驱动，
`QueueManager::fromEnv()` 则从 `QUEUE_DRIVER` / `QUEUE_HOST` / `QUEUE_PORT` … 读取（12-Factor 部署）。

## 核心概念

### Job：不可变的任务描述

```php
use Kode\Queue\Enum\Priority;
use Kode\Queue\Message\Job;

$job = Job::create('mail.send', ['to' => 'a@example.com'], queue: 'high', maxAttempts: 5);

$job->id;          // 01KZ62AAPPME5D6FFNXF9ZMF3G —— ULID
$job->name;        // mail.send
$job->payload;     // ['to' => 'a@example.com']
$job->attempts;    // 已尝试次数（由驱动在 reserve 时自增）
$job->priority;    // Priority::Normal

$high = $job->withPriority(Priority::High);   // 返回新实例，$job 本身不变
$job['job'];       // 'mail.send' —— v1 数组写法仍可读，写入会抛 LogicException
```

### ReservedJob：带回执的预留任务

`pop()` 拿到的不是裸 `Job`，而是 `ReservedJob`。`receipt` 是驱动侧的回执句柄
（Redis 是成员值、数据库是行 ID、Beanstalkd 是 job handle），后续 ack / release / fail 全靠它定位。

```php
$reserved = $queue->pop('high', timeout: 5.0);   // timeout > 0 时使用阻塞拉取

if ($reserved !== null) {
    try {
        handle($reserved->job->payload);
        $queue->ack($reserved);                   // 确认，任务彻底删除
    } catch (Throwable $e) {
        $queue->release($reserved, delay: 30);    // 30 秒后重投
        // 或 $queue->fail($reserved, $e);        // 放弃，交给死信
    }
}
```

### 可见性超时

任务被 `pop()` 出来后进入「预留」状态，在 `retry_after`（默认 90 秒）内对其他 Worker 不可见。
如果 Worker 中途崩溃，没人 ack 也没人 release，超时后任务自动重新可见：

```php
$queue->reclaimExpired('high');   // 手动回收；Worker 每 60 秒自动调用一次
```

**推论**：`retry_after` 必须大于任务最长执行时间，否则任务会被重复消费。

**release 的原子性**：放回队列是一次「比较并交换」——只有**当前预留记录还在、且 attempts 未被
其他 Worker 改动**时 release 才生效（Redis 侧由 Lua 脚本保证，Database 侧由带条件的 UPDATE 保证）。
预留记录已被回收（例如 Worker 超时后任务被别的进程取走）时 release 返回 `false`，
而不是把别人正在跑的任务拽回来。Beanstalkd / AMQP 的重投走「先投递后删除」，
重试负载携带已自增的 attempts，杜绝无限重投风暴。

## 投递任务

### 四种投递方式

```php
// 1. 最短写法
$queue->push('mail.send', ['to' => 'a@example.com']);

// 2. 指定队列
$queue->pushOn('high', 'mail.send', ['to' => 'a@example.com']);

// 3. 延迟：秒数 / DateInterval / DateTimeInterface 都收
$queue->later(60, 'mail.send', $data);
$queue->later(new DateInterval('PT10M'), 'mail.send', $data);
$queue->later(new DateTimeImmutable('tomorrow 09:00'), 'mail.send', $data);

// 4. 流式：所有参数一次说清
$id = $queue->job('order.settle', ['id' => 9527])
    ->onQueue('high')
    ->delay(30)
    ->priority(Priority::High)
    ->retries(5, BackoffStrategy::Fibonacci, base: 10)
    ->timeout(120)
    ->withHeaders(['tenant' => 'acme'])
    ->traceId($currentTraceId)
    ->dispatchIf($order->needsSettle);      // 条件投递，不满足返回 null
```

### 批量投递

```php
$ids = $queue->bulk(['mail.send', 'sms.send', 'push.send'], ['user_id' => 42]);

// 也可以每条带自己的数据
$ids = $queue->bulk([
    ['job' => 'mail.send', 'data' => ['to' => 'a@example.com']],
    ['job' => 'mail.send', 'data' => ['to' => 'b@example.com']],
], queue: 'mails');
```

支持 `Capability::Batch` 的驱动（Redis / Database / Kafka / Memory）会走原生批量管道，
其余驱动自动降级为逐条投递 —— 调用方无需分支。

#### 批量延迟投递

`bulkLater()` 与 `bulk()` 同款三种条目形态，但每条都带可见延迟（默认统一延迟，
数组条目可用 `'delay'` 键单独覆盖）。落盘后按 `availableAt` 排序，到期才进入就绪队列。

```php
// 30 秒后统一可见
$ids = $queue->bulkLater(30, ['mail.send', 'sms.send']);

// 每条各自延迟：b 1 秒后可见，a 仍用统一的 60 秒
$ids = $queue->bulkLater(60, [
    ['job' => 'a', 'data' => ['k' => 1]],
    ['job' => 'b', 'data' => ['k' => 2], 'delay' => 1],
]);
```

### 用 `#[AsJob]` 把参数绑在任务类上

v1 的痛点是同一个任务在十个地方投递，就有十份可能不一致的 `['queue' => ..., 'max_attempts' => ...]`。
v2 把这些参数声明在类上，投递方只管投：

```php
use Kode\Queue\Attribute\AsJob;
use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Enum\Priority;

#[AsJob(
    name: 'mail.welcome',
    queue: 'mails',
    maxAttempts: 5,
    priority: Priority::High,
    timeout: 120,
    backoff: BackoffStrategy::ExponentialJitter,
    unique: true,                       // 配合 IdempotencyMiddleware 去重
)]
final class SendWelcomeMail
{
    public function __construct(private readonly Mailer $mailer) {}

    public function handle(array $payload): void
    {
        $this->mailer->send($payload['to']);
    }
}

$queue->push(SendWelcomeMail::class, ['to' => 'a@example.com']);
// 队列 mails、5 次重试、High 优先级、120 秒超时 —— 全部自动带上
```

处理器方法按 `handle()` → `__invoke()` → `run()` → `execute()` 顺序探测。
构造函数依赖会通过 PSR-11 容器解析（装了 kode/di 就自动接上）。

## 消费任务

### 方式一：内建 Worker（推荐）

```php
use Kode\Queue\Config\WorkerOptions;
use Kode\Queue\Failed\FileFailedJobStore;
use Kode\Queue\Worker;

$worker = Worker::for(
    queue: $queue,
    handlers: [
        'mail.send' => fn (array $p) => Mailer::send($p['to']),
        SendWelcomeMail::class,          // 类名直接给，任务名从 #[AsJob] 读
    ],
    options: WorkerOptions::daemon('high', 'default'),
    failedStore: new FileFailedJobStore('/var/lib/kode-queue/failed'),
);

$summary = $worker->run();

echo $summary;
// [kode-worker@web-01] limit | 处理 1000（成功 986 / 失败 9 / 重投 5 / 回收 0）| 运行 742.3s | 峰值内存 61.2MB
```

`queues` 的顺序就是**严格优先级**：先把 `high` 抽干，再看 `default`。
只有最后一个队列会用阻塞拉取，避免高优先级队列被阻塞调用饿死。

### WorkerOptions

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `name` | `kode-worker` | Worker 标识，会写进日志与事件 |
| `queues` | `['default']` | 监听队列，顺序即优先级 |
| `sleep` | `1.0` | 队列为空时休眠秒数 |
| `blockTimeout` | `5.0` | 阻塞拉取超时 |
| `maxJobs` | `0` | 处理 N 个任务后退出（0 = 不限） |
| `maxTime` | `0` | 运行 N 秒后退出 |
| `memoryLimit` | `128` | 内存超过 N MB 后退出 |
| `maxAttempts` | `0` | **全局重试上限**（0 = 完全由任务自己声明） |
| `backoff` | `null` | 覆盖任务的退避策略（不设则沿用任务自带） |
| `nonRetryableExceptions` | `[]` | 命中即**直接落死信**、不重试（如 `UnsupportedFeatureException`），避免重试风暴 |
| `stopWhenEmpty` | `false` | 队列跑空即退出（CI / 补数场景） |
| `reclaimEvery` | `60.0` | 多久回收一次过期预留任务 |
| `reclaimJitter` | `0.0` | 回收前的随机错峰秒数（>0 时多 Worker 错峰，防惊群） |

两个开箱预设：

```php
WorkerOptions::batch('reports');   // stopWhenEmpty=true，跑完就退出
WorkerOptions::daemon('high');     // maxJobs=1000, maxTime=3600, memoryLimit=256
```

> **重试语义**：任务自带的 `maxAttempts` **优先**，Worker 的 `maxAttempts` 只作为全局天花板
> （两者取较小值）。这样 Worker 的配置不会让某个任务精心声明的 `maxAttempts: 10` 永久失效。
> 退避同理：`WorkerOptions::$backoff` 不设置时沿用任务自带策略。

> **布尔开关写字符串**：从 CLI / env 进来的值都是字符串，`fromArray()` 会识别
> `true/1/yes/on` 与 `false/0/no/off`（不区分大小写）。直接 `(bool) 'false'` 会得到 true 的老坑不再存在。

### 生命周期与信号

| 信号 | 行为 |
| --- | --- |
| `SIGTERM` / `SIGINT` / `SIGQUIT` | **优雅退出**：当前任务跑完再停，绝不半路丢任务 |
| `SIGUSR2` | 暂停 / 恢复（切换） |
| `SIGCONT` | 恢复 |

`Job::$timeout` 由 `pcntl_alarm()` 强制兑现 —— 处理器卡死会抛 `JobTimeoutException`，
而不是把 Worker 永久挂住。没有 `ext-pcntl`（常驻 Swoole / Swow 协程、或没编译该扩展）时，
自动降级为**软超时兜底**：把任务跑完再比对耗时，一旦超限同样抛 `JobTimeoutException`。
软超时无法中途打断执行，但至少保证「跑飞的任务」不会带着副作用被误判成功。

三条自愈红线（`maxJobs` / `maxTime` / `memoryLimit`）任一触发即干净退出，
交给 supervisor / systemd / k8s 重新拉起，从根上规避 PHP 长驻进程的内存碎片问题。

```ini
; supervisor 示例
[program:kode-queue]
command=/usr/bin/php /app/vendor/bin/kode-queue work --bootstrap=/app/queue.php --queue=high,default
numprocs=4
autorestart=true
stopsignal=TERM
stopwaitsecs=120        ; 大于最长任务耗时，给优雅退出留足时间
```

### 方式二：手动循环（完全掌控）

```php
foreach ($queue->consume('high', limit: 100, timeout: 1.0) as $reserved) {
    try {
        handle($reserved->job);
        $queue->ack($reserved);
    } catch (Throwable $e) {
        $queue->release($reserved, delay: 30);
    }
}
```

`consume()` 是 `Generator`，队列为空时按 `timeout` 阻塞等待，`limit` 到达后自然结束。

### 批量取出（popMany）

高吞吐场景一次往返取多条，减少网络/系统调用往返；返回的是**已预留**的任务，必须像 `pop()` 那样逐条 `ack()`，未确认的在可见性超时后由 Worker 回收。

```php
$reserved = $queue->popMany(50, 'high', timeout: 1.0);

foreach ($reserved as $job) {
    try {
        handle($job->job);
        $queue->ack($job);
    } catch (Throwable $e) {
        $queue->release($job, delay: 30);
    }
}
```

> `MemoryDriver` 走原生多取；不支持批量取数的驱动（如 `amqp`）按 `max` 逐条取，调用方无感。

### 运行时观测

```php
$worker->currentJob();   // ?ReservedJob，正在处理什么
$worker->snapshot();     // WorkerSummary，实时计数
$worker->pause();        // 暂停取新任务（当前任务不受影响）
$worker->stop('deploy'); // 请求优雅退出，附带原因
```

### 内建指标采集

给 Worker 挂一个 `MetricsMiddleware`，`pop` / `ack` / `release` / `fail` 的耗时与错误数会被自动累计，无需在业务里散落埋点：

```php
use Kode\Queue\Middleware\MetricsMiddleware;
use Kode\Queue\Worker;

$metrics = new MetricsMiddleware(
    sink: fn (string $op, float $ms, bool $failed) => Prometheus::observe($op, $ms, $failed),
);

$worker = Worker::for($queue, handlers: $handlers, metrics: $metrics);
$worker->run();

// 程序内取结构化快照，或渲染成人类可读摘要
$worker->metricsSnapshot();   // array<op, {count, errors, error_rate, avg_ms, max_ms, min_ms}>
echo $metrics->summary();     // 含 TOTAL 行的文本
echo $metrics->toPrometheus(); // Prometheus 文本格式
```

Worker 正常退出时也会把指标摘要写进日志（INFO 级）。

## 配置

### 三种来源

```php
QueueManager::make($config);   // 数组
QueueManager::fromEnv();       // 环境变量 QUEUE_*
QueueManager::auto();          // 零配置：Redis → Database → Memory 自动挑
```

### 完整配置示例

```php
$manager = QueueManager::make([
    'default' => 'redis',
    'connections' => [
        'redis' => [
            'driver'      => 'redis',
            'host'        => '127.0.0.1',
            'port'        => 6379,
            'password'    => null,
            'database'    => 0,
            'client'      => 'auto',      // auto | phpredis | predis
            'queue'       => 'default',
            'serializer'  => 'json',      // json | igbinary | composite
            'retry_after' => 90,          // 可见性超时（秒）
            'dead_letter' => true,        // 驱动层死信链表
        ],
        'mysql' => [
            'driver'       => 'database',
            'dsn'          => 'mysql:host=127.0.0.1;dbname=app;charset=utf8mb4',
            'username'     => 'root',
            'password'     => '',
            'table'        => 'jobs',
            'failed_table' => 'failed_jobs',
            'auto_migrate' => true,       // 首次使用自动建表 + 建索引
            'skip_locked'  => true,       // MySQL 8 / PG 9.5+ 用 SKIP LOCKED 抢占
        ],
        'beanstalk' => [
            'driver' => 'beanstalkd',
            'host'   => '127.0.0.1',
            'port'   => 11300,
            'tube'   => 'default',
        ],
        'rabbit' => [
            'driver'       => 'amqp',
            'host'         => '127.0.0.1',
            'port'         => 5672,
            'username'     => 'guest',
            'password'     => 'guest',
            'vhost'        => '/',
            'exchange'     => '',
            'durable'      => true,
            'prefetch'     => 1,
            'max_priority' => 10,
            'delayed_plugin' => false,    // 装了 rabbitmq_delayed_message_exchange 就打开
        ],
        'kafka' => [
            'driver'            => 'kafka',
            'bootstrap_servers' => '127.0.0.1:9092',
            'topic'             => 'kode-queue',
            'group_id'          => 'kode-consumer',
            'auto_offset_reset' => 'earliest',
            'topic_prefix'      => 'app.',
        ],
        'memory' => ['driver' => 'memory'],   // 单元测试
        'sync'   => ['driver' => 'sync'],     // 本地开发：投递即执行
    ],
]);

$manager->connection('mysql')->push('report.build', $data);
```

### 环境变量

| 变量 | 说明 |
| --- | --- |
| `QUEUE_DRIVER` | 驱动名，默认 `redis` |
| `QUEUE_HOST` / `QUEUE_PORT` | 主机与端口 |
| `QUEUE_PASSWORD` / `QUEUE_USERNAME` | 凭据 |
| `QUEUE_DATABASE` / `QUEUE_DSN` / `QUEUE_TABLE` | 数据库相关 |
| `QUEUE_NAME` | 默认队列名 |
| `QUEUE_SERIALIZER` | 序列化器 |
| `QUEUE_RETRY_AFTER` | 可见性超时 |

前缀可改：`QueueConfig::fromEnv('MYAPP_QUEUE_')`。
注意 `"0"` 是合法取值（如 `QUEUE_RETRY_AFTER=0`），不会被当成「未设置」而回落默认值。

### 敏感信息保护

`ConnectionConfig::secret()` 用 `#[SensitiveParameter]` 标注，密码不会出现在异常堆栈里；
`toSafeArray()` / `diagnose()` 输出的配置一律脱敏：

```php
$manager->diagnose();
// ['redis' => ['available' => true, 'driver' => 'redis',
//              'capabilities' => [...], 'config' => ['password' => '***']]]
```

## 驱动能力矩阵

后端能力天然有差异，v2 不假装抹平，而是让差异**可编程**：

| 驱动 | delay | priority | batch | ack | release | bury | clear | size | blocking | transaction | peek |
| --- | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
| **redis** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **database** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | — | ✅ | ✅ |
| **beanstalkd** | ✅ | ✅ | — | ✅ | ✅ | ✅ | — | ✅ | ✅ | — | ✅ |
| **amqp** | — | ✅ | — | ✅ | ✅ | — | ✅ | ✅ | ✅ | — | — |
| **kafka** | — | — | ✅ | ✅ | — | — | — | — | ✅ | — | — |
| **memory** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | — | — | ✅ |
| **sync** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | — | — | ✅ |
| **null** | — | — | — | ✅ | — | — | ✅ | ✅ | — | — | — |

```php
use Kode\Queue\Enum\Capability;

if ($queue->supports(Capability::Delay)) {
    $queue->later(60, 'mail.send', $data);
} else {
    $scheduler->at(now()->addMinute(), fn () => $queue->push('mail.send', $data));
}
```

不支持的能力被调用时抛 `UnsupportedFeatureException`，异常消息里直接写明「换哪个驱动能用」。

**选型速查**：

- **redis** —— 默认首选。全能力、Lua 脚本保证原子性、性能最好。
- **database** —— 已有 MySQL/PG 又不想加组件时用；`SKIP LOCKED` 让多 Worker 抢占不打架。
- **beanstalkd** —— 想要原生 bury / kick 语义、追求极低延迟。
- **amqp** —— 已有 RabbitMQ 基建、需要 exchange 路由与多语言互通。
- **kafka** —— 超高吞吐日志流场景；**没有单条 ack**，语义偏消息流而非任务队列。
- **memory / sync / null** —— 单元测试、本地开发、临时禁用队列。

## 失败、重试与死信

### 退避策略

| 策略 | 第 1/2/3/4 次（base=5） | 适用 |
| --- | --- | --- |
| `Fixed` | 5, 5, 5, 5 | 下游恢复时间可预期 |
| `Linear` | 5, 10, 15, 20 | 温和递增 |
| `Exponential` | 5, 10, 20, 40 | 下游疑似过载 |
| `ExponentialJitter`（默认） | 5±, 10±, 20±, 40± | **避免重试风暴**，大量任务同时失败时首选 |
| `Fibonacci` | 5, 10, 15, 25 | 介于线性与指数之间 |

```php
$queue->job('sync.remote', $data)
      ->retries(5, BackoffStrategy::ExponentialJitter, base: 10)
      ->dispatch();
```

所有策略都有上限封顶，不会算出「三天后重试」这种结果。

### 死信存储

尝试次数耗尽后，任务连同异常链完整落入死信：

```php
use Kode\Queue\Failed\ArrayFailedJobStore;      // 进程内，单元测试用
use Kode\Queue\Failed\FileFailedJobStore;       // 文件，中小项目零依赖
use Kode\Queue\Failed\DatabaseFailedJobStore;   // 数据库，多机共享

$store = new FileFailedJobStore('/var/lib/kode-queue/failed');
$store = DatabaseFailedJobStore::fromDsn('mysql:host=...;dbname=app', 'root', '');
```

三者接口一致：

```php
$store->all(queue: 'mails', limit: 50, offset: 0);   // 按失败时间倒序
$store->find($id);        // 完整记录：job / exception / trace / connection
$store->count('mails');
$store->forget($id);
$store->flush(queue: 'mails', olderThan: time() - 86400 * 7);
```

`FileFailedJobStore` 的实现细节：按队列名分目录（清洗名 + SHA1 前 8 位防冲突），
ULID 文件名倒序即时间倒序，写入走「临时文件 + `rename()`」原子提交 —— 并发写不会读到半截 JSON。

### 重放

```bash
vendor/bin/kode-queue failed:list  --bootstrap=queue.php
vendor/bin/kode-queue failed:show  01KZ62AAPT... --bootstrap=queue.php    # 完整异常堆栈
vendor/bin/kode-queue failed:retry 01KZ62AAPT... --bootstrap=queue.php
vendor/bin/kode-queue failed:retry all --bootstrap=queue.php --queue=mails
vendor/bin/kode-queue failed:flush --bootstrap=queue.php
```

`failed:retry` 重放前会把 attempts **清零**（`Job::resetAttempts()`）：人工修好问题后重放，
期望是「完整重试预算的新执行」，而不是继承死信时的计数、被重试天花板直接秒拒。

## 中间件

中间件包住**队列操作**（push / pop / ack / …），按 `priority` 从小到大执行。

| 中间件 | 作用 |
| --- | --- |
| `RetryMiddleware` | 驱动层瞬时故障重试（连接抖动），区别于任务级重试 |
| `RateLimitMiddleware` | 令牌桶限流，装了 kode/limiting 自动切分布式实现 |
| `CircuitBreakerMiddleware` | 熔断：连续失败达阈值后快速失败，冷却后半开探测 |
| `IdempotencyMiddleware` | 幂等去重，需要 PSR-16 缓存 |
| `LoggingMiddleware` | PSR-3 日志，可设慢操作阈值 |
| `MetricsMiddleware` | 指标埋点，回调形态对接 Prometheus / StatsD |
| `ContextMiddleware` | 透传 trace_id / tenant 等上下文，装了 kode/context 自动接管 |

```php
use Kode\Queue\Middleware\{CircuitBreakerMiddleware, LoggingMiddleware, RateLimitMiddleware};

$manager->pushMiddleware(
    new LoggingMiddleware($logger, slowThresholdMs: 200.0),
    new RateLimitMiddleware(capacity: 100, rate: 50.0),
    new CircuitBreakerMiddleware(failureThreshold: 5, cooldownSeconds: 30.0),
);

// 也可以只给某个连接加，门面不可变，返回新实例
$critical = $queue->withMiddleware(new CircuitBreakerMiddleware())
                  ->withoutMiddleware(RateLimitMiddleware::class);
```

### 自定义中间件

```php
use Kode\Queue\Middleware\{AbstractMiddleware, Operation};

final class TenantMiddleware extends AbstractMiddleware
{
    public function handle(Operation $operation, callable $next): mixed
    {
        if ($operation->name === Operation::PUSH && $operation->job !== null) {
            $operation = $operation->withJob(
                $operation->job->withHeaders(['tenant' => Tenant::current()]),
            );
        }

        return $next($operation);
    }
}
```

`Operation` 携带 `name` / `queue` / `connection` / `arguments` / `job` / `reserved` / `context`，
同样是不可变对象，改动一律走 `withJob()` / `withContext()` / `withArguments()`。

## 事件

六个生命周期事件，全部是 `readonly class`：

| 事件 | 触发时机 | 关键字段 |
| --- | --- | --- |
| `JobQueued` | 任务入队后 | `job`, `connection` |
| `JobProcessing` | 开始处理前 | `reserved`, `worker` |
| `JobProcessed` | 处理成功后 | `reserved`, `worker`, `durationMs`, `result` |
| `JobFailed` | 处理抛异常 | `reserved`, `exception`, `worker`, `willRetry` |
| `JobReleased` | 失败后重投（将重试） | `reserved`, `exception`, `worker`, `delay` |
| `WorkerStarting` | Worker 启动 | `options`, `pid` |
| `WorkerPaused` | 收到暂停信号 / 显式暂停 | `worker`, `reason` |
| `WorkerResumed` | 收到恢复信号 / 显式恢复 | `worker`, `reason` |
| `WorkerStopped` | Worker 退出 | `worker`, `reason`, `processed`, `failed`, `uptimeSeconds` |

```php
use Kode\Queue\Contract\EventDispatcherInterface;
use Kode\Queue\Event\JobFailed;

final class Metrics implements EventDispatcherInterface
{
    public function dispatch(object $event): object
    {
        if ($event instanceof JobFailed && !$event->willRetry) {
            Prometheus::counter('queue_job_dead')->inc([$event->reserved->job->queue]);
        }

        return $event;
    }
}

$manager->withEvents(new Metrics());
```

任意 PSR-14 派发器都能直接传入；装了 **kode/event** 则完全无需接线，自动探测并接管。

## 定时任务（Scheduler）

基于 delayed 队列实现 cron 风格定点投递，**零依赖**，无需引入额外调度器。

```php
use Kode\Queue\Scheduler;

$scheduler = Scheduler::for($queue)
    ->cron('0 9 * * *', 'report.daily')                 // 每天 9:00 跑日报
    ->cron('*/5 * * * *', 'heartbeat', ['region' => 'cn']) // 每 5 分钟心跳
    ->at(new DateTimeImmutable('2026-09-01 00:00'), 'bill.settle') // 绝对时刻一次性
    ->every(3600, 'cache.warm')                          // 固定间隔（秒）
    ->tick(3600);                                        // 守护：每 3600 秒唤醒一次，到点就投递

// 单进程常驻
$scheduler->run(300);   // 每 300 秒轮询一次，内部按最短间隔对齐
```

- `cron()` 支持标准 5 字段（`分 时 日 月 周`，`*` `,` `-` `/` 步长，`7` 等同周日 `0`）；
- `every()` 用相对秒数，适合「每隔 N 秒」这类简单节奏；
- `tick()` / `run()` 返回本次触发的投递数；注入时间可测试（见 `SchedulerTest`）。

CLI 直接托管一个调度定义文件：

```bash
vendor/bin/kode-queue schedule --schedule=scheduler.php --bootstrap=queue.php
```

`scheduler.php` 返回 `Scheduler` 实例即可：

```php
<?php
use Kode\Queue\Scheduler;
return Scheduler::for($queue)->cron('0 9 * * *', 'report.daily');
```

## 命令行工具

```bash
vendor/bin/kode-queue <命令> [参数] [选项]
```

| 命令 | 说明 |
| --- | --- |
| `work` | 启动 Worker 消费队列 |
| `size` | 查看队列积压数量 |
| `stats` | 查看各状态统计（就绪 / 延迟 / 预留 / 合计） |
| `diagnose` | 诊断各连接可用性与能力 |
| `flush` | 清空实时队列（调用 `clear`） |
| `benchmark` | 吞吐压测（`--jobs=N` 投递数，`--consume` 顺带消费掉这批） |
| `schedule` | 运行定时任务守护（`--schedule=FILE`） |
| `failed:list` / `failed:show` | 列出 / 查看失败任务 |
| `failed:retry` / `failed:forget` / `failed:flush` | 重放 / 删除 / 清空 |

`--bootstrap` 指向一个 PHP 文件，返回 `QueueManager` 或配置数组：

```php
<?php
// queue.php
use Kode\Queue\QueueManager;

return [
    'manager'  => QueueManager::fromEnv(),      // 也可以直接给 default / connections
    'failed'   => '/var/lib/kode-queue/failed', // 目录 → 文件存储；也可传 FailedJobStoreInterface
    'handlers' => [
        'mail.send'  => fn (array $p) => Mailer::send($p['to']),
        'report.run' => ReportRunner::class,
    ],
];
```

```bash
# 常驻消费，high 优先于 default
vendor/bin/kode-queue work --bootstrap=queue.php --queue=high,default

# CI / 补数：跑空就退出
vendor/bin/kode-queue work --bootstrap=queue.php --stop-when-empty

# 只处理一个任务（调试）
vendor/bin/kode-queue work --bootstrap=queue.php --once

# 处理 500 个或 10 分钟后退出，交给 supervisor 重启
vendor/bin/kode-queue work --bootstrap=queue.php --max-jobs=500 --max-time=600 --memory=256
```

`work` 的退出码：全部干净完成为 `0`，存在失败任务或异常退出为 `1` —— 可直接用于 CI 断言。

```bash
# 清空 default 队列（危险操作，先确认队列名）
vendor/bin/kode-queue flush --bootstrap=queue.php --queue=default

# 压测：投递 1 万条并消费掉，报告吞吐
vendor/bin/kode-queue benchmark --jobs=10000 --consume --bootstrap=queue.php
```

## 与 kode 生态集成

全部是**软依赖**：装了自动生效，没装静默降级，代码不用改。

| 包 | 装上之后 | 没装时 |
| --- | --- | --- |
| **kode/event** | 队列事件自动派发到全局事件总线 | 内部 `NullEventDispatcher` |
| **kode/context** | `ContextMiddleware` 自动透传 trace_id / tenant，跨协程不串号 | 仅透传显式声明的 headers |
| **kode/di** | 任务处理器支持构造函数依赖注入 | 只支持无参构造与闭包 |
| **kode/cache** | `IdempotencyMiddleware` 用分布式缓存去重 | 内置 `ArrayCache`（进程内） |
| **kode/limiting** | `RateLimitMiddleware` 用分布式限流 | 内置令牌桶（进程内） |

```php
use Kode\Queue\Integration\{KodeContextBridge, KodeEventBridge, KodeRuntimeBridge};

KodeEventBridge::detect();        // 自动找到可用派发器，找不到返回 Null 实现
KodeContextBridge::isAvailable(); // kode/context 是否就位
KodeRuntimeBridge::detect();      // 'swoole' | 'swow' | 'fiber' | 'cli' | 'fpm'
```

桥接层全部用 `class_exists()` 探测，**不会**在 composer 里制造硬依赖。

## 序列化

| 序列化器 | 说明 |
| --- | --- |
| `json`（默认） | 可读、跨语言，用 `json_validate()` 预校验（PHP 8.3 新函数，比 try-catch 快） |
| `igbinary` | 体积约为 JSON 的 1/2，速度约 2 倍，需要 `ext-igbinary` |
| `composite` | 写用 igbinary、读时自动识别，**支持在线平滑切换** |

```php
'connections' => [
    'redis' => ['driver' => 'redis', 'serializer' => 'composite'],
],
```

`composite` 的用途：老数据是 JSON、新数据要换 igbinary，切换期两种格式并存也能正常读，
等队列里的旧消息消费干净后再改成 `igbinary` 即可。

## 协程与 Swoole

`KodeRuntimeBridge` 自动识别运行时，Worker 休眠时让出调度权而非阻塞：

```php
Co\run(function () use ($queue) {
    for ($i = 0; $i < 4; $i++) {
        Co\go(fn () => Worker::for($queue, $handlers)->run());
    }
});
```

注意：`pcntl` 信号处理在协程环境下不可用，`KodeRuntimeBridge::supportsSignals()` 会返回 `false`，
Worker 自动降级为「靠 `maxJobs` / `maxTime` 主动退出」，不会尝试注册信号导致报错。

## 测试

用 `memory` 驱动，无需任何外部服务：

```php
use Kode\Queue\Failed\ArrayFailedJobStore;
use Kode\Queue\QueueManager;
use Kode\Queue\Worker;
use Kode\Queue\Config\WorkerOptions;

$queue = QueueManager::make([
    'default' => 'memory',
    'connections' => ['memory' => ['driver' => 'memory']],
])->default();

$queue->push('mail.send', ['to' => 'a@example.com']);

self::assertSame(1, $queue->size());

$failed = new ArrayFailedJobStore();
$summary = Worker::for($queue, ['mail.send' => $spy], WorkerOptions::batch(), $failed)->run();

self::assertSame(1, $summary->processed);
self::assertSame(0, $summary->failed);
self::assertTrue($summary->isClean());
```

本地开发想让任务立即执行、跳过队列？把驱动换成 `sync` 就行，API 完全一致。

```bash
composer test           # PHPUnit
composer test-coverage  # 覆盖率报告
composer lint           # 全量语法检查
```

## v1 → v2 迁移指南

v2 是**破坏性升级**。好消息是改动集中在入口与消费端，业务处理逻辑基本不用动。

### 1. 类映射

| v1 | v2 | 说明 |
| --- | --- | --- |
| `Kode\Queue\Factory::create($config)` | `Kode\Queue\QueueManager::make($config)` | 工厂改名，同时管理多连接 |
| `Factory::createWithDriver('redis', $opts)` | `QueueManager::make(['default' => 'redis', 'connections' => [...]])->default()` | 显式化 |
| `Kode\Queue\QueueInterface` | `Kode\Queue\Contract\QueueInterface` | 移入 `Contract\` |
| `Kode\Queue\Driver\DriverInterface` | `Kode\Queue\Contract\DriverInterface` | 同上 |
| `Kode\Queue\Middleware\MiddlewareInterface` | `Kode\Queue\Contract\MiddlewareInterface` | 同上 |
| `Kode\Queue\AbstractQueue` | 已删除 | 用 `Queue` + 自定义驱动替代 |
| `Kode\Queue\Context\Context` | `Kode\Queue\Integration\KodeContextBridge` | 改为桥接 kode/context |
| `Kode\Queue\Util\QueueUtil` | 拆分为 `Support\IdGenerator`、`Enum\BackoffStrategy`、`Message\Job` | 职责归位 |
| `Kode\Queue\Middleware\LogMiddleware` | `Kode\Queue\Middleware\LoggingMiddleware` | 改用 PSR-3 |

### 2. 出队语义变了（最重要）

```php
// v1：pop 直接删除，进程一崩任务就没了
$job = $queue->pop();
if ($job) {
    handle($job['job'], $job['data']);
}

// v2：pop 只是「预留」，必须显式 ack
$reserved = $queue->pop();
if ($reserved !== null) {
    try {
        handle($reserved->job->name, $reserved->job->payload);
        $queue->ack($reserved);                 // ← 不写这行，任务超时后会被重投
    } catch (Throwable $e) {
        $queue->release($reserved, delay: 30);
    }
}
```

**这是 v2 最容易踩的坑**：忘记 `ack()`，任务会在 `retry_after` 后重新可见并被再次消费。
用内建 `Worker` 就不必操心，它会自动 ack / release / 落死信。

### 3. 数组下标仍然可读

为降低迁移成本，`Job` 实现了 `ArrayAccess`，v1 的读法继续有效：

```php
$reserved->job['job'];    // 任务名（等价 ->name）
$reserved->job['data'];   // 负载（等价 ->payload）
```

但**写入会抛 `LogicException`** —— `Job` 是不可变对象，请改用 `withPayload()` 等方法。

### 4. 配置键名调整

- 连接配置必须显式写 `'driver' => 'redis'`（v1 靠连接名猜）
- `max_attempts` 从投递参数变为任务属性：优先用 `#[AsJob(maxAttempts: 5)]` 或 `->retries(5)`
- 新增 `retry_after`（可见性超时，默认 90 秒）—— **务必设为大于最长任务耗时**

### 5. 分步迁移建议

1. 升级 PHP 到 8.3，`composer require kode/queue:^2.0`
2. 全局替换 `Factory::create` → `QueueManager::make`，补上 `driver` 键
3. 消费端替换为 `Worker`（推荐）或手工补齐 `ack()` / `release()`
4. 配上死信存储，`kode-queue failed:list` 观察一段时间
5. 把散落在投递点的 `max_attempts` / `queue` 参数收敛到 `#[AsJob]`

需要回滚？v1 最后一个版本是 `v1.4.0`，`composer require kode/queue:^1.4` 即可。

## 目录结构

```
src/
├── Attribute/      #[AsJob] 属性
├── Config/         QueueConfig / ConnectionConfig / WorkerOptions
├── Console/        零依赖 CLI（Application / StdoutLogger）
├── Contract/       全部接口（Queue / Driver / Serializer / FailedJobStore / …）
├── Driver/         8 个驱动 + Redis 客户端适配层（phpredis / predis）
├── Enum/           Capability / DriverType / Priority / JobStatus / BackoffStrategy
├── Event/          6 个生命周期事件
├── Exception/      异常体系，全部实现 QueueExceptionInterface
├── Failed/         死信存储：Array / File / Database
├── Integration/    kode 生态桥接（软依赖探测）
├── Message/        Job / ReservedJob（readonly）
├── Middleware/     7 个内置中间件 + Pipeline + Operation
├── Serializer/     json / igbinary / composite
├── Support/        IdGenerator（ULID）/ QueueStats / WorkerSummary
├── Queue.php            队列门面（不可变，withXxx 派生）
├── QueueManager.php     连接管理与驱动扩展
├── PendingDispatch.php  流式投递
├── HandlerResolver.php  处理器解析（handle/__invoke/run/execute）
└── Worker.php           消费循环
```

## 许可证

[Apache-2.0](LICENSE) © KodePHP Team
