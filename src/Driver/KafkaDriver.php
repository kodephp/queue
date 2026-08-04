<?php

declare(strict_types=1);

namespace Kode\Queue\Driver;

use Kode\Queue\Enum\Capability;
use Kode\Queue\Enum\DriverType;
use Kode\Queue\Exception\DriverException;
use Kode\Queue\Exception\UnsupportedFeatureException;
use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;
use Kode\Queue\Support\QueueStats;
use Override;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message as KafkaMessage;
use RdKafka\Producer;
use RdKafka\TopicPartition;

/**
 * Apache Kafka 驱动。
 *
 * Kafka 是日志流而不是任务队列，把它当队列用时有两条硬约束必须讲清楚，
 * 而不是像 v1 那样悄悄降级：
 *
 * - **没有延迟投递**。分区是追加日志，无法插队。传入 delay 时直接抛
 *   {@see UnsupportedFeatureException}，让调用方明确改用 Redis / 数据库连接，
 *   而不是让任务「看起来投递成功了，其实立刻就被消费」。
 * - **没有单条重回队列**。`release()` 只能重新生产一条新消息，
 *   因此消息 offset 会前进——这是 Kafka 的语义，不是缺陷。
 *
 * 能给到的保证是：手动提交位点（`enable.auto.commit = false`），
 * 处理成功才 commit，Worker 崩溃时该分区会从上次提交处重放。
 */
class KafkaDriver extends AbstractDriver
{
    private ?Producer $producer = null;

    private ?KafkaConsumer $consumer = null;

    private ?string $subscribed = null;

    #[Override]
    public function type(): DriverType
    {
        return DriverType::Kafka;
    }

    // ------------------------------------------------------------------
    // 生产
    // ------------------------------------------------------------------

    #[Override]
    public function push(Job $job): string
    {
        $job = $this->normalize($job);

        if ($job->delaySeconds() > 0) {
            throw UnsupportedFeatureException::for($this->type()->value, Capability::Delay);
        }

        $this->guard('push', function () use ($job): void {
            $producer = $this->producer();
            $topic = $producer->newTopic($this->topic($job->queue));

            $topic->produce(
                RD_KAFKA_PARTITION_UA,
                0,
                $this->encode($job),
                // 用任务名做 key，保证同类任务落到同一分区、保持相对顺序
                $this->config->bool('key_by_name', true) ? $job->name : $job->id,
            );

            $producer->poll(0);
        }, ['job' => $job->id, 'topic' => $this->topic($job->queue)]);

        return $job->id;
    }

    #[Override]
    public function pushBatch(iterable $jobs): array
    {
        $ids = [];

        foreach ($jobs as $job) {
            $ids[] = $this->push($job);
        }

        // 批量场景统一 flush，避免逐条等待 broker 确认
        $this->flush();

        return $ids;
    }

    /**
     * 等待本地发送队列清空（进程退出前必须调用，否则消息会丢在内存里）。
     */
    public function flush(?int $timeoutMs = null): void
    {
        $this->producer?->flush($timeoutMs ?? $this->config->int('flush_timeout', 5000));
    }

    // ------------------------------------------------------------------
    // 消费
    // ------------------------------------------------------------------

    #[Override]
    public function pop(string $queue, float $timeout = 0.0): ?ReservedJob
    {
        return $this->guard('pop', function () use ($queue, $timeout): ?ReservedJob {
            $consumer = $this->consumer($queue);
            $timeoutMs = $timeout > 0
                ? (int) ($timeout * 1000)
                : $this->config->int('consume_timeout', 1000);

            $message = $consumer->consume($timeoutMs);

            return match ($message->err) {
                RD_KAFKA_RESP_ERR_NO_ERROR => $this->wrap($message),
                RD_KAFKA_RESP_ERR__PARTITION_EOF, RD_KAFKA_RESP_ERR__TIMED_OUT => null,
                default => throw DriverException::operationFailed(
                    'kafka.consume',
                    new \RuntimeException((string) $message->errstr(), (int) $message->err),
                    ['topic' => $this->topic($queue)],
                ),
            };
        }, ['queue' => $queue]);
    }

    #[Override]
    public function ack(ReservedJob $job): bool
    {
        return $this->guard('ack', function () use ($job): bool {
            $message = $job->meta['message'] ?? null;

            if (!$message instanceof KafkaMessage) {
                return false;
            }

            // 同步提交，确保「处理成功」与「位点前进」不会因崩溃而错位
            $this->consumer?->commit($message);

            return true;
        }, ['job' => $job->job->id]);
    }

    /**
     * Kafka 无法把已消费的 offset 退回，只能重新生产一条。
     */
    #[Override]
    public function release(ReservedJob $job, int $delay = 0): bool
    {
        if ($delay > 0) {
            throw UnsupportedFeatureException::for($this->type()->value, Capability::Delay);
        }

        return $this->guard('release', function () use ($job): bool {
            $this->push($job->job);
            $this->ack($job);

            return true;
        }, ['job' => $job->job->id]);
    }

    /**
     * 失败消息转投死信 topic（默认 `{topic}.dlq`），再提交原位点。
     */
    #[Override]
    public function fail(ReservedJob $job, \Throwable $reason): bool
    {
        return $this->guard('fail', function () use ($job, $reason): bool {
            $dlq = $this->config->string('dlq_topic', $this->topic($job->job->queue) . '.dlq');

            if ($dlq !== null && $dlq !== '') {
                $producer = $this->producer();
                $producer->newTopic($dlq)->produce(
                    RD_KAFKA_PARTITION_UA,
                    0,
                    json_encode([
                        ...$job->job->toArray(),
                        'failed_at' => time(),
                        'exception' => $reason::class,
                        'message' => $reason->getMessage(),
                    ], JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                    $job->job->id,
                );
                $producer->poll(0);
            }

            return $this->ack($job);
        }, ['job' => $job->job->id]);
    }

    // ------------------------------------------------------------------
    // 管理
    // ------------------------------------------------------------------

    /**
     * 消费滞后量（lag）：高水位 - 已提交位点，是 Kafka 场景下最接近「队列长度」的指标。
     */
    #[Override]
    public function size(string $queue): int
    {
        return $this->guard('size', function () use ($queue): int {
            $consumer = $this->consumer($queue);
            $topic = $this->topic($queue);
            $timeoutMs = $this->config->int('metadata_timeout', 2000);

            $metadata = $consumer->getMetadata(false, $consumer->newTopic($topic), $timeoutMs);
            $partitions = [];

            foreach ($metadata->getTopics() as $topicMetadata) {
                if ($topicMetadata->getTopic() !== $topic) {
                    continue;
                }
                foreach ($topicMetadata->getPartitions() as $partition) {
                    $partitions[] = new TopicPartition($topic, $partition->getId());
                }
            }

            if ($partitions === []) {
                return 0;
            }

            $lag = 0;
            foreach ($consumer->getCommittedOffsets($partitions, $timeoutMs) as $committed) {
                $low = 0;
                $high = 0;
                $consumer->queryWatermarkOffsets(
                    $topic,
                    $committed->getPartition(),
                    $low,
                    $high,
                    $timeoutMs,
                );

                $offset = $committed->getOffset();
                $lag += $offset >= 0 ? max(0, $high - $offset) : max(0, $high - $low);
            }

            return $lag;
        }, ['queue' => $queue]);
    }

    #[Override]
    public function stats(string $queue): QueueStats
    {
        return QueueStats::of(
            queue: $queue,
            driver: $this->type()->value,
            ready: $this->size($queue),
            extra: [
                'topic' => $this->topic($queue),
                'group_id' => $this->config->string('group_id', 'kode-queue'),
            ],
        );
    }

    // ------------------------------------------------------------------
    // 连接
    // ------------------------------------------------------------------

    #[Override]
    public function isConnected(): bool
    {
        return $this->producer !== null || $this->consumer !== null;
    }

    #[Override]
    public function close(): void
    {
        $this->flush();

        try {
            $this->consumer?->close();
        } catch (\Throwable) {
            // 忽略关闭异常
        }

        $this->producer = null;
        $this->consumer = null;
        $this->subscribed = null;
        $this->connected = false;
    }

    public function producer(): Producer
    {
        if ($this->producer instanceof Producer) {
            return $this->producer;
        }

        $this->connected = true;

        return $this->producer = new Producer($this->conf());
    }

    public function consumer(string $queue): KafkaConsumer
    {
        $topic = $this->topic($queue);

        if ($this->consumer instanceof KafkaConsumer && $this->subscribed === $topic) {
            return $this->consumer;
        }

        if ($this->consumer instanceof KafkaConsumer) {
            $this->consumer->unsubscribe();
        } else {
            $conf = $this->conf();
            $conf->set('group.id', $this->config->string('group_id', 'kode-queue') ?? 'kode-queue');
            $conf->set('auto.offset.reset', $this->config->string('auto_offset_reset', 'earliest') ?? 'earliest');
            // 关闭自动提交：位点必须由 ack() 显式推进
            $conf->set('enable.auto.commit', 'false');
            $conf->set('enable.partition.eof', 'true');

            $this->consumer = new KafkaConsumer($conf);
            $this->connected = true;
        }

        $this->consumer->subscribe([$topic]);
        $this->subscribed = $topic;

        return $this->consumer;
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private function conf(): Conf
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->config->string('bootstrap_servers', '127.0.0.1:9092') ?? '127.0.0.1:9092');
        $conf->set('socket.keepalive.enable', 'true');

        if (($username = $this->config->string('username')) !== null && $username !== '') {
            $conf->set('sasl.username', $username);
            $conf->set('sasl.password', $this->config->secret('password', '') ?? '');
            $conf->set('sasl.mechanisms', $this->config->string('sasl_mechanism', 'PLAIN') ?? 'PLAIN');
            $conf->set('security.protocol', $this->config->string('security_protocol', 'SASL_PLAINTEXT') ?? 'SASL_PLAINTEXT');
        }

        /** @var array<string, scalar> $extra */
        $extra = $this->config->array('conf');
        foreach ($extra as $key => $value) {
            $conf->set((string) $key, (string) $value);
        }

        return $conf;
    }

    private function wrap(KafkaMessage $message): ReservedJob
    {
        $payload = (string) $message->payload;
        $job = $this->decode($payload)->incrementAttempts();

        return ReservedJob::of(
            job: $job,
            receipt: sprintf('%d:%d', $message->partition, $message->offset),
            rawPayload: $payload,
            meta: [
                'message' => $message,
                'partition' => $message->partition,
                'offset' => $message->offset,
            ],
        );
    }

    private function topic(string $queue): string
    {
        $prefix = $this->config->string('topic_prefix', '') ?? '';
        $base = $this->config->string('topic');

        // 未显式指定 topic 时，队列名即 topic 名
        return $prefix . ($base !== null && $base !== '' && $queue === 'default' ? $base : $queue);
    }
}
