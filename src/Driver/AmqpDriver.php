<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Enum\Capability;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Enum\JobStatus;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;
use Override;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * AMQP / RabbitMQ 驱动。
 *
 * 三个 v1 未解决的问题在这里补齐：
 *
 * 1. **延迟投递**。AMQP 协议本身没有延迟消息。v2 用「TTL 队列 + 死信交换机」
 *    实现：延迟任务先投进一个带 `x-message-ttl` 的中转队列，到期后由 RabbitMQ
 *    自动死信回主队列。若集群装了 `rabbitmq_delayed_message_exchange` 插件，
 *    则自动改用插件的 `x-delay` 头，开销更低。
 * 2. **真正的 ack**。v1 用 `basic_get(no_ack = true)`，消息一取出就从 broker 消失，
 *    Worker 崩溃即丢消息。v2 改为手动确认，配合 `basic_nack` 重回队列。
 * 3. **优先级**。声明队列时带上 `x-max-priority`，并把内部 0-100 的优先级
 *    线性映射到 AMQP 的 0-10。
 */
class AmqpDriver extends AbstractDriver
{
    private ?AbstractConnection $connection = null;

    private ?AMQPChannel $channel = null;

    /** @var array<string, true> 本进程已声明过的队列，避免重复 declare */
    private array $declared = [];

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Amqp;
    }

    #[Override]
    public function supports(Capability $capability): bool
    {
        // 延迟能力由本驱动通过 DLX/插件补齐，不在枚举的「原生能力」清单里
        return $capability === Capability::Delay || parent::supports($capability);
    }

    // ------------------------------------------------------------------
    // 生产
    // ------------------------------------------------------------------

    #[Override]
    public function push(Job $job): string
    {
        $job = $this->normalize($job);

        $this->guard('push', function () use ($job): void {
            $delay = $job->delaySeconds();
            $channel = $this->channel();
            $this->declareQueue($job->queue);

            $properties = [
                'delivery_mode' => $this->config->bool('durable', true)
                    ? AMQPMessage::DELIVERY_MODE_PERSISTENT
                    : AMQPMessage::DELIVERY_MODE_NON_PERSISTENT,
                'content_type' => 'application/json',
                'message_id' => $job->id,
                'timestamp' => $job->createdAt !== 0 ? $job->createdAt : time(),
                'priority' => $job->priority->toAmqp(),
                'application_headers' => new AMQPTable(array_filter([
                    'x-queue' => $job->queue,
                    'x-attempts' => $job->attempts,
                    'x-trace-id' => $job->traceId(),
                ], static fn (mixed $v): bool => $v !== null)),
            ];

            [$exchange, $routingKey] = $this->route($job->queue, $delay, $properties);

            $channel->basic_publish(
                new AMQPMessage($this->encode($job), $properties),
                $exchange,
                $routingKey,
            );
        }, ['job' => $job->id, 'queue' => $job->queue]);

        return $job->id;
    }

    #[Override]
    public function pushBatch(iterable $jobs): array
    {
        $ids = [];

        $this->guard('pushBatch', function () use ($jobs, &$ids): void {
            $channel = $this->channel();

            foreach ($jobs as $job) {
                $ids[] = $this->push($job);
            }

            // 一次性 flush，省掉逐条的网络往返
            $channel->publish_batch();
        });

        return $ids;
    }

    // ------------------------------------------------------------------
    // 消费
    // ------------------------------------------------------------------

    #[Override]
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob
    {
        return $this->guard('pop', function () use ($queue, $timeout): ?ReservedJob {
            $deadline = microtime(true) + max(0.0, $timeout);
            $this->declareQueue($queue);

            do {
                $message = $this->channel()->basic_get($queue, false);

                if ($message instanceof AMQPMessage) {
                    $payload = $message->getBody();
                    $job = $this->decode($payload)->incrementAttempts();

                    return ReservedJob::of(
                        job: $job,
                        receipt: (string) $message->getDeliveryTag(),
                        rawPayload: $payload,
                        meta: ['message' => $message, 'redelivered' => $message->isRedelivered()],
                    );
                }

                $remaining = $deadline - microtime(true);
                if ($remaining <= 0) {
                    break;
                }

                usleep((int) (min(0.05, $remaining) * 1_000_000));
            } while (microtime(true) < $deadline);

            return null;
        }, ['queue' => $queue]);
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        return $this->guard('ack', function () use ($job): bool {
            $this->channel()->basic_ack($this->tagOf($job));

            return true;
        }, ['job' => $job->job->id]);
    }

    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        return $this->guard('release', function () use ($job, $delay): bool {
            $channel = $this->channel();

            // 统一走 ack + 重新投递：nack(requeue) 回投的是原始消息体，
            // attempts 会在每次 pop 时从旧负载重新 +1，maxAttempts 永远达不到，
            // 反复失败的任务将形成无限重试风暴。$job->job 已含自增后的 attempts。
            $channel->basic_ack($this->tagOf($job));
            $this->push($delay > 0 ? $job->job->delayedBy($delay) : $job->job->withStatus(JobStatus::Pending));

            return true;
        }, ['job' => $job->job->id, 'delay' => $delay]);
    }

    /**
     * 失败即 `basic_nack(requeue = false)`：
     * 队列若配置了 `x-dead-letter-exchange`，消息会自动进入死信队列。
     */
    #[Override]
    public function fail(ReservedJob $job, \Throwable $reason): bool
    {
        return $this->guard('fail', function () use ($job): bool {
            $this->channel()->basic_nack($this->tagOf($job), false, false);

            return true;
        }, ['job' => $job->job->id]);
    }

    // ------------------------------------------------------------------
    // 管理
    // ------------------------------------------------------------------

    #[Override]
    public function size(string $queue): int
    {
        return $this->guard('size', function () use ($queue): int {
            $this->declareQueue($queue);
            // passive = true：只查询不创建
            $result = $this->channel()->queue_declare($queue, true);

            return \is_array($result) ? (int) ($result[1] ?? 0) : 0;
        }, ['queue' => $queue]);
    }

    #[Override]
    public function clear(string $queue): int
    {
        return $this->guard('clear', function () use ($queue): int {
            $size = $this->size($queue);
            $this->channel()->queue_purge($queue);

            return $size;
        }, ['queue' => $queue]);
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        return $this->guard('stats', function () use ($queue): QueueStats {
            $this->declareQueue($queue);
            $result = $this->channel()->queue_declare($queue, true);
            $delayed = 0;

            foreach (array_keys($this->declared) as $name) {
                if (str_starts_with($name, $queue . '.delay.')) {
                    $sub = $this->channel()->queue_declare($name, true);
                    $delayed += \is_array($sub) ? (int) ($sub[1] ?? 0) : 0;
                }
            }

            return QueueStats::of(
                queue: $queue,
                driver: $this->type()->value,
                ready: \is_array($result) ? (int) ($result[1] ?? 0) : 0,
                delayed: $delayed,
                extra: [
                    'consumers' => \is_array($result) ? (int) ($result[2] ?? 0) : 0,
                    'exchange' => $this->config->string('exchange', ''),
                ],
            );
        }, ['queue' => $queue]);
    }

    // ------------------------------------------------------------------
    // 连接
    // ------------------------------------------------------------------

    #[Override]
    public function isConnected(): bool
    {
        return $this->connection?->isConnected() ?? false;
    }

    #[Override]
    public function reconnect(): void
    {
        $this->close();
        $this->channel();
    }

    #[Override]
    public function close(): void
    {
        try {
            $this->channel?->close();
            $this->connection?->close();
        } catch (\Throwable) {
            // 连接已断开，无需处理
        }

        $this->channel = null;
        $this->connection = null;
        $this->declared = [];
        $this->connected = false;
    }

    public function channel(): AMQPChannel
    {
        if ($this->channel instanceof AMQPChannel && $this->connection?->isConnected() === true) {
            return $this->channel;
        }

        $injected = $this->config->object('connection');

        $this->connection = $injected instanceof AbstractConnection
            ? $injected
            : new AMQPStreamConnection(
                host: $this->config->string('host', '127.0.0.1') ?? '127.0.0.1',
                port: $this->config->int('port', 5672),
                user: $this->config->string('username', 'guest') ?? 'guest',
                password: $this->config->secret('password', 'guest') ?? 'guest',
                vhost: $this->config->string('vhost', '/') ?? '/',
                connection_timeout: $this->config->float('timeout', 3.0),
                read_write_timeout: $this->config->float('read_write_timeout', 130.0),
                keepalive: $this->config->bool('keepalive', true),
                heartbeat: $this->config->int('heartbeat', 30),
            );

        $this->connected = true;
        $channel = $this->connection->channel();

        // QoS：一次只预取配置的条数，避免单个 Worker 把整个队列抓走
        if (($prefetch = $this->config->int('prefetch', 1)) > 0) {
            $channel->basic_qos(0, $prefetch, false);
        }

        return $this->channel = $channel;
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /**
     * 计算投递目标：普通消息直投主队列，延迟消息走插件或 TTL 中转队列。
     *
     * @param  array<string, mixed> $properties 会被就地补上 x-delay 头
     * @return array{0: string, 1: string}      [exchange, routingKey]
     */
    private function route(string $queue, int $delay, array &$properties): array
    {
        $exchange = $this->config->string('exchange', '') ?? '';

        if ($delay <= 0) {
            return [$exchange, $queue];
        }

        if ($this->config->bool('delayed_plugin')) {
            $headers = $properties['application_headers'];
            \assert($headers instanceof AMQPTable);
            $headers->set('x-delay', $delay * 1000);

            return [$this->config->string('delayed_exchange', 'kode.delayed') ?? 'kode.delayed', $queue];
        }

        return ['', $this->declareDelayQueue($queue, $delay)];
    }

    /**
     * 声明主队列（含优先级上限与可选死信交换机）。
     */
    private function declareQueue(string $queue): void
    {
        if (isset($this->declared[$queue])) {
            return;
        }

        $arguments = [];

        if (($maxPriority = $this->config->int('max_priority', 10)) > 0) {
            $arguments['x-max-priority'] = ['I', min(255, $maxPriority)];
        }

        if (($dlx = $this->config->string('dead_letter_exchange')) !== null && $dlx !== '') {
            $arguments['x-dead-letter-exchange'] = ['S', $dlx];
        }

        $this->channel()->queue_declare(
            queue: $queue,
            passive: false,
            durable: $this->config->bool('durable', true),
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: new AMQPTable($arguments),
        );

        $this->declared[$queue] = true;
    }

    /**
     * 声明「延迟中转队列」：消息在此静置 TTL 秒后死信回主队列。
     *
     * 延迟时长按 10 秒粒度归桶，避免为每个不同延迟都建一条队列。
     */
    private function declareDelayQueue(string $queue, int $delay): string
    {
        $bucket = max(1, (int) ceil($delay / 10) * 10);
        $name = sprintf('%s.delay.%d', $queue, $bucket);

        if (!isset($this->declared[$name])) {
            $this->declareQueue($queue);
            $this->channel()->queue_declare(
                queue: $name,
                passive: false,
                durable: $this->config->bool('durable', true),
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: new AMQPTable([
                    'x-message-ttl' => ['I', $bucket * 1000],
                    'x-dead-letter-exchange' => ['S', $this->config->string('exchange', '') ?? ''],
                    'x-dead-letter-routing-key' => ['S', $queue],
                    // 中转队列长时间无消息即自动清理，防止桶数无限膨胀
                    'x-expires' => ['I', ($bucket + 3600) * 1000],
                ]),
            );

            $this->declared[$name] = true;
        }

        return $name;
    }

    private function tagOf(ReservedJob $job): int|string
    {
        $message = $job->meta['message'] ?? null;

        return $message instanceof AMQPMessage ? $message->getDeliveryTag() : $job->receipt;
    }
}
