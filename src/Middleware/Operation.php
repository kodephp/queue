<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Kode\Queue\Message\Job;
use Kode\Queue\Message\ReservedJob;

/**
 * 一次队列操作的描述（不可变）。
 *
 * 取代 v1 的 `(string $method, array $parameters)` 二元组：
 * 中间件不用再靠位置索引猜参数含义，也不用担心某个驱动改了参数顺序就全线崩溃。
 */
final readonly class Operation
{
    /** 已知操作名（类型化常量，PHP 8.3） */
    public const string PUSH = 'push';
    public const string PUSH_BATCH = 'pushBatch';
    public const string POP = 'pop';
    public const string PEEK = 'peek';
    public const string ACK = 'ack';
    public const string RELEASE = 'release';
    public const string FAIL = 'fail';
    public const string SIZE = 'size';
    public const string CLEAR = 'clear';
    public const string STATS = 'stats';

    /**
     * @param string               $name       操作名
     * @param string               $queue      目标队列
     * @param string               $connection 连接名
     * @param list<mixed>          $arguments  传给驱动方法的实参
     * @param Job|null             $job        涉及的任务（push 类操作）
     * @param ReservedJob|null     $reserved   涉及的预留任务（ack/release/fail）
     * @param array<string, mixed> $context    中间件之间传递的附加上下文
     */
    public function __construct(
        public string $name,
        public string $queue,
        public string $connection = 'default',
        public array $arguments = [],
        public ?Job $job = null,
        public ?ReservedJob $reserved = null,
        public array $context = [],
    ) {
    }

    /**
     * @param list<mixed> $arguments
     */
    public static function of(
        string $name,
        string $queue,
        string $connection = 'default',
        array $arguments = [],
        ?Job $job = null,
        ?ReservedJob $reserved = null,
    ): self {
        return new self($name, $queue, $connection, $arguments, $job, $reserved);
    }

    /** 是否为写操作（生产侧） */
    public function isWrite(): bool
    {
        return \in_array($this->name, [self::PUSH, self::PUSH_BATCH, self::RELEASE], true);
    }

    /** 是否为读操作（消费侧） */
    public function isRead(): bool
    {
        return \in_array($this->name, [self::POP, self::PEEK, self::SIZE, self::STATS], true);
    }

    /**
     * 限流 / 幂等 / 熔断的默认分组键。
     */
    public function key(): string
    {
        return "{$this->connection}:{$this->queue}:{$this->name}";
    }

    /**
     * @param array<string, mixed> $context
     */
    public function withContext(array $context): self
    {
        return new self(
            $this->name,
            $this->queue,
            $this->connection,
            $this->arguments,
            $this->job,
            $this->reserved,
            [...$this->context, ...$context],
        );
    }

    public function withJob(Job $job): self
    {
        return new self(
            $this->name,
            $this->queue,
            $this->connection,
            $this->arguments,
            $job,
            $this->reserved,
            $this->context,
        );
    }

    /**
     * @param list<mixed> $arguments
     */
    public function withArguments(array $arguments): self
    {
        return new self(
            $this->name,
            $this->queue,
            $this->connection,
            $arguments,
            $this->job,
            $this->reserved,
            $this->context,
        );
    }
}
