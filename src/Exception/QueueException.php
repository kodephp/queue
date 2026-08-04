<?php

declare(strict_types=1);

namespace Kode\Queue\Exception;

use RuntimeException;

/**
 * 队列组件所有异常的基类。
 *
 * 相比旧版的裸 `\Exception`，这里补充了「结构化上下文」，
 * 便于与 kode/exception 的统一异常处理器、链路追踪对接。
 */
class QueueException extends RuntimeException implements QueueExceptionInterface
{
    /**
     * @param array<string, mixed> $context 结构化上下文（会被日志/追踪原样记录）
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        ?\Throwable $previous = null,
        protected readonly array $context = [],
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * 结构化上下文。
     *
     * @return array<string, mixed>
     */
    public function context(): array
    {
        return $this->context;
    }

    /**
     * 追加上下文并返回新实例（异常本身保持不可变语义）。
     *
     * @param array<string, mixed> $context
     */
    public function withContext(array $context): static
    {
        return new static(
            $this->getMessage(),
            $this->getCode(),
            $this->getPrevious(),
            [...$this->context, ...$context],
        );
    }

    /**
     * 导出为数组，方便直接进日志。
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'type' => static::class,
            'message' => $this->getMessage(),
            'code' => $this->getCode(),
            'file' => $this->getFile(),
            'line' => $this->getLine(),
            'context' => $this->context,
            'previous' => $this->getPrevious()?->getMessage(),
        ];
    }
}
