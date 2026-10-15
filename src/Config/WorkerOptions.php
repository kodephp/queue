<?php

declare(strict_types=1);

namespace Kode\Queue\Config;

use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Exception\UnsupportedFeatureException;

/**
 * Worker 运行参数（不可变）。
 *
 * 长驻进程最怕的三件事——内存泄漏、僵尸任务、重试风暴——
 * 分别由 `memoryLimit`、`timeout`、`backoff` 三个参数兜底。
 */
final readonly class WorkerOptions
{
    /**
     * @param string          $name           Worker 名称（出现在日志与心跳中）
     * @param list<string>    $queues         监听的队列，按顺序优先消费
     * @param float           $sleep          队列为空时的休眠秒数
     * @param float           $blockTimeout   阻塞拉取超时（支持长轮询的驱动才生效）
     * @param int             $maxJobs        处理多少个任务后自动退出，0 = 不限
     * @param int             $maxTime        运行多少秒后自动退出，0 = 不限
     * @param int             $memoryLimit    内存超过多少 MB 后自动退出，0 = 不限
     * @param int             $maxAttempts    全局重试次数上限，0 = 不封顶。
     *                                        任务自带的 `maxAttempts` 优先；两者都设置时取更严的那个。
     *                                        它的意义是给「声明了无限重试」的任务兜个底，避免重试风暴。
     * @param BackoffStrategy $backoff        退避策略覆盖，null = 沿用任务自带的策略
     * @param int             $backoffBase    退避基础间隔（秒）覆盖，0 = 沿用任务自带的值
     * @param bool            $stopWhenEmpty  队列空时立即退出（适合一次性批处理 / CI）。
     *                                        注意：被延迟重投的任务此刻不算「就绪」，Worker 会直接退出。
     * @param bool            $catchThrowable 是否连 Error 一起捕获（false 时 Error 记录死信后终止进程）
     * @param int             $restartSignal  收到该信号后优雅退出
     * @param float           $reclaimEvery   多久扫描一次超时预留任务（秒），0 = 关闭
     * @param float           $reclaimJitter  回收时的随机抖动上限（秒），0 = 不抖动。
     *                                        多个 Worker 同时回收孤儿预留任务时，给每个进程加一点随机错峰，
     *                                        避免「惊群」把后端打成一个尖峰。仅对本地内存驱动无感（单进程）。
     * @param list<class-string<\Throwable>> $nonRetryableExceptions 这类异常一旦抛出就直接进死信，
     *                                        不再按 maxAttempts 重试。默认含 {@see UnsupportedFeatureException}：
     *                                        「这个驱动根本不支持该功能」重试一百次也不会忽然支持，纯属浪费。
     */
    public function __construct(
        public string $name = 'kode-worker',
        public array $queues = ['default'],
        public private(set) float $sleep = 1.0,
        public private(set) float $blockTimeout = 5.0,
        public private(set) int $maxJobs = 0,
        public private(set) int $maxTime = 0,
        public private(set) int $memoryLimit = 128,
        public int $maxAttempts = 0,
        public ?BackoffStrategy $backoff = null,
        public int $backoffBase = 0,
        public private(set) bool $stopWhenEmpty = false,
        public private(set) bool $catchThrowable = true,
        public private(set) int $restartSignal = 15, // SIGTERM
        public private(set) float $reclaimEvery = 60.0,
        public private(set) float $reclaimJitter = 0.0,
        public array $nonRetryableExceptions = [UnsupportedFeatureException::class],
    ) {
    }

    /**
     * @param array<string, mixed> $options
     */
    public static function fromArray(array $options): self
    {
        $queues = $options['queues'] ?? $options['queue'] ?? ['default'];

        return new self(
            name: (string) ($options['name'] ?? 'kode-worker'),
            queues: array_values(array_map(
                strval(...),
                \is_array($queues) ? $queues : explode(',', (string) $queues),
            )),
            sleep: (float) ($options['sleep'] ?? 1.0),
            blockTimeout: (float) ($options['block_timeout'] ?? 5.0),
            maxJobs: (int) ($options['max_jobs'] ?? 0),
            maxTime: (int) ($options['max_time'] ?? 0),
            memoryLimit: (int) ($options['memory_limit'] ?? 128),
            maxAttempts: (int) ($options['max_attempts'] ?? 0),
            backoff: BackoffStrategy::tryFrom((string) ($options['backoff'] ?? '')),
            backoffBase: (int) ($options['backoff_base'] ?? 0),
            stopWhenEmpty: self::toBool($options['stop_when_empty'] ?? false, false),
            catchThrowable: self::toBool($options['catch_throwable'] ?? true, true),
            restartSignal: (int) ($options['restart_signal'] ?? 15),
            reclaimEvery: (float) ($options['reclaim_every'] ?? 60.0),
            reclaimJitter: (float) ($options['reclaim_jitter'] ?? 0.0),
            nonRetryableExceptions: self::parseExceptions($options['non_retryable'] ?? null),
        );
    }

    /** 一次性批处理预设：把队列跑空就退出 */
    public static function batch(string ...$queues): self
    {
        return new self(
            queues: $queues === [] ? ['default'] : array_values($queues),
            sleep: 0.0,
            blockTimeout: 0.0,
            stopWhenEmpty: true,
            reclaimEvery: 0.0,
        );
    }

    /** 常驻守护预设：长轮询 + 定期自愈重启 */
    public static function daemon(string ...$queues): self
    {
        return new self(
            queues: $queues === [] ? ['default'] : array_values($queues),
            blockTimeout: 5.0,
            maxJobs: 1000,
            maxTime: 3600,
            memoryLimit: 256,
        );
    }

    public function withQueues(string ...$queues): self
    {
        return $this->copy(queues: array_values($queues));
    }

    public function withName(string $name): self
    {
        return $this->copy(name: $name);
    }

    /** 是否达到退出条件 */
    public function shouldStop(int $processed, float $startedAt): bool
    {
        if ($this->maxJobs > 0 && $processed >= $this->maxJobs) {
            return true;
        }

        if ($this->maxTime > 0 && (microtime(true) - $startedAt) >= $this->maxTime) {
            return true;
        }

        return $this->memoryLimit > 0
            && memory_get_usage(true) / 1024 / 1024 >= $this->memoryLimit;
    }

    private function copy(
        ?string $name = null,
        ?array $queues = null,
    ): self {
        return new self(
            name: $name ?? $this->name,
            queues: $queues ?? $this->queues,
            sleep: $this->sleep,
            blockTimeout: $this->blockTimeout,
            maxJobs: $this->maxJobs,
            maxTime: $this->maxTime,
            memoryLimit: $this->memoryLimit,
            maxAttempts: $this->maxAttempts,
            backoff: $this->backoff,
            backoffBase: $this->backoffBase,
            stopWhenEmpty: $this->stopWhenEmpty,
            catchThrowable: $this->catchThrowable,
            restartSignal: $this->restartSignal,
            reclaimEvery: $this->reclaimEvery,
            reclaimJitter: $this->reclaimJitter,
            nonRetryableExceptions: $this->nonRetryableExceptions,
        );
    }

    /**
     * 从配置里解析「不可重试异常」清单。
     *
     * 允许传逗号分隔的字符串（CLI 场景）或字符串数组；非法条目直接丢弃，
     * 避免一个拼写错误就让整个 Worker 起不来。
     */
    /**
     * 布尔归一化。
     *
     * CLI / env 传来的都是字符串，直接 `(bool) 'false'` 会得到 true，
     * 相当于开关永远打不开——这里显式识别常见的假值写法。
     */
    private static function toBool(mixed $value, bool $default): bool
    {
        if (\is_bool($value)) {
            return $value;
        }

        if (\is_string($value)) {
            return match (strtolower(trim($value))) {
                'true', '1', 'yes', 'on' => true,
                'false', '0', 'no', 'off' => false,
                default => $default,
            };
        }

        return \is_int($value) ? $value !== 0 : $default;
    }

    private static function parseExceptions(mixed $value): array
    {
        if ($value === null) {
            return [UnsupportedFeatureException::class];
        }

        $raw = \is_array($value) ? $value : explode(',', (string) $value);
        $clean = [];

        foreach ($raw as $item) {
            $item = trim((string) $item);
            if ($item !== '') {
                $clean[] = $item;
            }
        }

        return $clean === [] ? [UnsupportedFeatureException::class] : $clean;
    }
}
