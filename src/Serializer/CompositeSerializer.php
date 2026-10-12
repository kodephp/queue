<?php

declare(strict_types=1);

namespace Kode\Queue\Serializer;

use Kode\Queue\Contract\SerializerInterface;
use Kode\Queue\Exception\SerializationException;
use Kode\Queue\Message\Job;
use Override;

/**
 * 复合序列化器：写用主序列化器，读时按魔数自动识别。
 *
 * 用途：从 JSON 切换到 igbinary（或反之）时，队列中还有旧格式的存量消息。
 * 挂上它就能做到「零停机、零丢消息」的平滑迁移。
 */
final class CompositeSerializer implements SerializerInterface
{
    /** @var list<SerializerInterface> */
    private readonly array $readers;

    /**
     * @param SerializerInterface   $writer  写入时使用
     * @param SerializerInterface ...$others 读取时的候选（自动包含 writer）
     */
    public function __construct(
        private readonly SerializerInterface $writer,
        SerializerInterface ...$others,
    ) {
        $this->readers = [$writer, ...$others];
    }

    #[Override]
    public function name(): string
    {
        return 'composite(' . implode('|', array_map(
            static fn (SerializerInterface $s): string => $s->name(),
            $this->readers,
        )) . ')';
    }

    #[Override]
    public function serialize(Job $job): string
    {
        return $this->writer->serialize($job);
    }

    #[Override]
    public function deserialize(string $raw): Job
    {
        foreach ($this->readers as $reader) {
            if ($reader->supports($raw)) {
                return $reader->deserialize($raw);
            }
        }

        throw SerializationException::decodeFailed($raw, $this->name());
    }

    #[Override]
    public function supports(string $raw): bool
    {
        return array_any($this->readers, fn($reader) => $reader->supports($raw));
    }
}
