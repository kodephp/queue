<?php

declare(strict_types=1);

namespace Kode\Queue\Cron;

use Kode\Queue\Exception\QueueException;

/**
 * cron 表达式语法错误。
 */
final class CronParseException extends QueueException
{
}
