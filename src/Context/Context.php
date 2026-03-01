<?php

namespace Kode\Queue\Context;

/**
 * 队列任务上下文类
 * 
 * 用于封装和管理队列任务的上下文信息，提供便捷的方法来访问和操作任务数据。
 * 在任务处理器中可以使用此类来获取任务的各项属性。
 */
class Context {
    /**
     * 任务数据
     */
    protected array $data = [];

    /**
     * 任务ID
     */
    protected ?string $jobId = null;

    /**
     * 队列名称
     */
    protected ?string $queue = null;

    /**
     * 重试次数
     */
    protected int $attempts = 0;

    /**
     * 创建时间戳
     */
    protected int $createdAt = 0;

    /**
     * 创建上下文实例
     *
     * @param array $data 任务数据
     */
    public function __construct(array $data = []) {
        $this->data = $data;
        $this->jobId = $data['id'] ?? null;
        $this->queue = $data['queue'] ?? null;
        $this->attempts = $data['attempts'] ?? 0;
        $this->createdAt = $data['created_at'] ?? time();
    }

    /**
     * 获取任务ID
     *
     * @return string|null 任务ID
     */
    public function getJobId(): ?string {
        return $this->jobId;
    }

    /**
     * 获取队列名称
     *
     * @return string|null 队列名称
     */
    public function getQueue(): ?string {
        return $this->queue;
    }

    /**
     * 获取重试次数
     *
     * @return int 重试次数
     */
    public function getAttempts(): int {
        return $this->attempts;
    }

    /**
     * 获取创建时间戳
     *
     * @return int 创建时间戳
     */
    public function getCreatedAt(): int {
        return $this->createdAt;
    }

    /**
     * 获取任务名称
     *
     * @return mixed 任务名称
     */
    public function getJob() {
        return $this->data['job'] ?? null;
    }

    /**
     * 获取任务数据
     *
     * @return array 任务数据
     */
    public function getData(): array {
        return $this->data['data'] ?? [];
    }

    /**
     * 获取完整的任务负载数据
     *
     * @return array 任务负载数据
     */
    public function getPayload(): array {
        return $this->data;
    }

    /**
     * 增加重试次数
     *
     * @return self 支持链式调用
     */
    public function incrementAttempts(): self {
        $this->attempts++;
        $this->data['attempts'] = $this->attempts;
        return $this;
    }

    /**
     * 转换为数组
     *
     * @return array 任务数据数组
     */
    public function toArray(): array {
        return $this->data;
    }

    /**
     * 转换为JSON字符串
     *
     * @return string JSON字符串
     */
    public function toJson(): string {
        return json_encode($this->data);
    }

    /**
     * 从数组创建上下文实例
     *
     * @param array $data 任务数据
     * @return self 上下文实例
     */
    public static function fromArray(array $data): self {
        return new self($data);
    }

    /**
     * 从JSON字符串创建上下文实例
     *
     * @param string $json JSON字符串
     * @return self 上下文实例
     */
    public static function fromJson(string $json): self {
        return new self(json_decode($json, true));
    }
}
