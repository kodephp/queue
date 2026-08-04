<?php

declare(strict_types=1);

namespace Kode\Queue\Tests\Fixtures;

use Kode\Queue\Attribute\AsJob;
use Kode\Queue\Enum\Priority;

/**
 * 带 `#[AsJob]` 的任务类：投递时自动套用属性里声明的队列 / 重试 / 优先级。
 */
#[AsJob(
    name: 'mail.send',
    queue: 'mails',
    maxAttempts: 2,
    priority: Priority::High,
    timeout: 0,
)]
final class SendMailJob
{
    /** @var list<string> */
    public static array $sent = [];

    public function handle(array $data): string
    {
        self::$sent[] = (string) ($data['to'] ?? '');

        return 'sent';
    }

    public static function reset(): void
    {
        self::$sent = [];
    }
}
