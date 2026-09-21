<?php

declare(strict_types=1);

namespace Kode\Queue\Support;

use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;

/**
 * 任务 ID 生成器。
 *
 * v1 使用 `uniqid('', true)`：基于系统时间且熵极低，高并发下会重复，且不可排序。
 * v2 改用 **ULID**（Universally Unique Lexicographically Sortable Identifier）：
 *
 * - 前 48 bit 为毫秒时间戳 → 字典序 == 时间序，天然适合做 Redis ZSet 成员、数据库主键；
 * - 后 80 bit 由 PHP 8.2 的 `Random\Randomizer` 提供的 CSPRNG 填充；
 * - Crockford Base32 编码，26 个字符，大小写不敏感、无易混淆字符。
 */
final class IdGenerator
{
    /** Crockford Base32 字母表（去掉 I、L、O、U） */
    public const string ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

    /** ULID 长度 */
    public const int ULID_LENGTH = 26;

    private static ?Randomizer $randomizer = null;

    /** 同毫秒内的单调递增计数，保证同一进程内严格有序 */
    private static int $lastTimestamp = 0;

    /** @var list<int> */
    private static array $lastRandom = [];

    private function __construct()
    {
    }

    /**
     * 生成一个 ULID。
     */
    public static function ulid(?int $millisecond = null): string
    {
        $timestamp = $millisecond ?? (int) (microtime(true) * 1000);

        // 同一毫秒内做单调自增，避免排序抖动（ULID 规范推荐做法）
        if ($timestamp === self::$lastTimestamp && self::$lastRandom !== []) {
            self::$lastRandom = self::incrementRandom(self::$lastRandom);
        } else {
            self::$lastTimestamp = $timestamp;
            self::$lastRandom = self::randomBytes(10);
        }

        return self::encodeTime($timestamp) . self::encodeRandom(self::$lastRandom);
    }

    /**
     * 生成 UUID v7（同样时间有序，便于与外部系统对接）。
     */
    public static function uuidV7(?int $millisecond = null): string
    {
        $timestamp = $millisecond ?? (int) (microtime(true) * 1000);
        $bytes = substr(pack('J', $timestamp), 2, 6) . self::randomizer()->getBytes(10);

        // 写入 version(7) 与 variant(RFC 4122)
        $bytes[6] = \chr((\ord($bytes[6]) & 0x0F) | 0x70);
        $bytes[8] = \chr((\ord($bytes[8]) & 0x3F) | 0x80);

        $hex = bin2hex($bytes);

        return sprintf(
            '%s-%s-%s-%s-%s',
            substr($hex, 0, 8),
            substr($hex, 8, 4),
            substr($hex, 12, 4),
            substr($hex, 16, 4),
            substr($hex, 20, 12),
        );
    }

    /**
     * 从 ULID 反解出毫秒时间戳。
     */
    public static function timestampOf(string $ulid): ?int
    {
        if (\strlen($ulid) !== self::ULID_LENGTH) {
            return null;
        }

        $timestamp = 0;
        foreach (str_split(strtoupper(substr($ulid, 0, 10))) as $char) {
            $index = strpos(self::ALPHABET, $char);
            if ($index === false) {
                return null;
            }
            $timestamp = $timestamp * 32 + $index;
        }

        return $timestamp;
    }

    /**
     * 校验是否为合法 ULID。
     */
    public static function isUlid(string $value): bool
    {
        return \strlen($value) === self::ULID_LENGTH
            && strspn(strtoupper($value), self::ALPHABET) === self::ULID_LENGTH;
    }

    /**
     * 短随机串（用于 consumer tag、worker name 等非持久标识）。
     */
    public static function token(int $length = 16): string
    {
        return substr(bin2hex(self::randomizer()->getBytes((int) ceil($length / 2))), 0, $length);
    }

    /**
     * fork 之后重新播种。
     *
     * 子进程会原样继承父进程的随机引擎状态与「同毫秒单调自增计数」，
     * 两个进程若在同一毫秒生成 ULID 会得到完全相同的 ID。
     * worker 派生子进程后必须调用本方法（KodeRuntimeBridge::reset 已挂钩）。
     */
    public static function reseedAfterFork(): void
    {
        self::$randomizer = new Randomizer(new Xoshiro256StarStar());
        self::$lastTimestamp = 0;
        self::$lastRandom = [];
    }

    /**
     * 注入自定义随机引擎——测试中可用确定性种子复现随机序列。
     */
    public static function setRandomizer(?Randomizer $randomizer): void
    {
        self::$randomizer = $randomizer;
        self::$lastTimestamp = 0;
        self::$lastRandom = [];
    }

    private static function randomizer(): Randomizer
    {
        return self::$randomizer ??= new Randomizer(new Xoshiro256StarStar());
    }

    /** @return list<int> */
    private static function randomBytes(int $count): array
    {
        return array_values(unpack('C*', self::randomizer()->getBytes($count)) ?: []);
    }

    /**
     * @param  list<int> $bytes
     * @return list<int>
     */
    private static function incrementRandom(array $bytes): array
    {
        for ($i = \count($bytes) - 1; $i >= 0; $i--) {
            if ($bytes[$i] < 255) {
                $bytes[$i]++;

                return $bytes;
            }
            $bytes[$i] = 0;
        }

        return $bytes;
    }

    private static function encodeTime(int $timestamp): string
    {
        $out = '';
        for ($i = 9; $i >= 0; $i--) {
            $out = self::ALPHABET[$timestamp % 32] . $out;
            $timestamp = intdiv($timestamp, 32);
        }

        return $out;
    }

    /** @param list<int> $bytes */
    private static function encodeRandom(array $bytes): string
    {
        // 10 字节 = 80 bit → 16 个 Base32 字符
        $bits = '';
        foreach ($bytes as $byte) {
            $bits .= str_pad(decbin($byte), 8, '0', STR_PAD_LEFT);
        }

        $out = '';
        foreach (str_split($bits, 5) as $chunk) {
            $out .= self::ALPHABET[bindec($chunk)];
        }

        return $out;
    }
}
