<?php

declare(strict_types=1);

namespace Kode\Queue\Console;

use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;
use Stringable;

/**
 * 命令行用的极简 PSR-3 日志器。
 *
 * 只做三件事：加时间戳、按级别着色、把 context 拼成一行。
 * 真要接 Monolog / kode/log，把它换掉即可——Worker 只认 PSR-3 接口。
 */
final class StdoutLogger extends AbstractLogger
{
    /** 级别 => ANSI 颜色码 */
    private const array COLORS = [
        LogLevel::EMERGENCY => '1;41',
        LogLevel::ALERT => '1;31',
        LogLevel::CRITICAL => '1;31',
        LogLevel::ERROR => '31',
        LogLevel::WARNING => '33',
        LogLevel::NOTICE => '36',
        LogLevel::INFO => '32',
        LogLevel::DEBUG => '90',
    ];

    /** 低于该级别的日志在 quiet 模式下被丢弃 */
    private const array QUIET_ALLOWED = [
        LogLevel::EMERGENCY,
        LogLevel::ALERT,
        LogLevel::CRITICAL,
        LogLevel::ERROR,
        LogLevel::WARNING,
    ];

    private readonly bool $colorize;

    /** @param resource $stream */
    public function __construct(
        private $stream = \STDOUT,
        private readonly bool $quiet = false,
        ?bool $colorize = null,
    ) {
        $this->colorize = $colorize ?? self::detectColorSupport($stream);
    }

    /**
     * @param array<string, mixed> $context
     */
    #[\Override]
    public function log($level, string|Stringable $message, array $context = []): void
    {
        $level = (string) $level;

        if ($this->quiet && !\in_array($level, self::QUIET_ALLOWED, true)) {
            return;
        }

        $line = sprintf(
            '[%s] %-9s %s%s',
            date('H:i:s'),
            strtoupper($level),
            (string) $message,
            $context === [] ? '' : ' ' . self::formatContext($context),
        );

        fwrite($this->stream, $this->paint($line, $level) . \PHP_EOL);
    }

    /**
     * @param array<string, mixed> $context
     */
    private static function formatContext(array $context): string
    {
        $parts = [];

        foreach ($context as $key => $value) {
            $parts[] = $key . '=' . match (true) {
                \is_scalar($value) => (string) $value,
                $value === null => 'null',
                $value instanceof Stringable => (string) $value,
                default => json_encode($value, \JSON_UNESCAPED_UNICODE) ?: '?',
            };
        }

        return '{' . implode(' ', $parts) . '}';
    }

    private function paint(string $line, string $level): string
    {
        if (!$this->colorize) {
            return $line;
        }

        $color = self::COLORS[$level] ?? '0';

        return "\033[{$color}m{$line}\033[0m";
    }

    /** @param resource $stream */
    private static function detectColorSupport($stream): bool
    {
        // 重定向到文件时不要写入转义序列，否则日志里全是乱码
        if (getenv('NO_COLOR') !== false) {
            return false;
        }

        return @stream_isatty($stream);
    }
}
