<?php

declare(strict_types=1);

namespace Kode\Queue\Contract;

use Kode\Queue\Message\Job;

/**
 * 负载序列化契约。
 *
 * 把「怎么编码」从驱动里剥离出来，于是同一个 Redis 驱动既能用 JSON（跨语言可读）
 * 也能用 igbinary（体积小 30%、快 2 倍），只需换一行配置。
 */
interface SerializerInterface
{
    /**
     * 序列化器标识（写入负载头部，反序列化时用于自检）。
     */
    public function name(): string;

    /**
     * 任务对象 → 字符串。
     *
     * @throws \Kode\Queue\Exception\SerializationException
     */
    public function serialize(Job $job): string;

    /**
     * 字符串 → 任务对象。
     *
     * @throws \Kode\Queue\Exception\SerializationException
     */
    public function deserialize(string $raw): Job;

    /**
     * 快速判断字符串是否由本序列化器产出（用于混合负载的平滑迁移）。
     */
    public function supports(string $raw): bool;
}
