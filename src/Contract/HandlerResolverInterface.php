<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

use Kode\Queue\Message\Job;

/**
 * 任务处理器解析契约。
 *
 * 决定「任务名 → 实际可执行代码」的映射方式：
 * 闭包注册表、PSR-11 容器（kode/di）、`#[AsJob]` 属性扫描，都可以是它的实现。
 */
interface HandlerResolverInterface
{
    /**
     * 能否解析该任务。
     */
    public function canResolve(Job $job): bool;

    /**
     * 解析为可调用体。
     *
     * @return callable(Job): mixed
     *
     * @throws \Kode\Queue\Exception\JobNotFoundException 无法解析时
     */
    public function resolve(Job $job): callable;
}
