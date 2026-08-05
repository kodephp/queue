<?php

declare(strict_types=1);

namespace Kode\Queue\Cron;

use DateTimeImmutable;
use DateTimeZone;

/**
 * 极简但完整的 5 字段 cron 表达式解析与「下次运行时间」计算。
 *
 * 字段顺序（与系统 crontab 一致）：**分 时 日 月 周**。
 * 支持语法：
 *
 * - `*`           该字段任意值
 * - `a,b,c`       列表，匹配其中任一
 * - `a-b`         闭区间
 * - `a/b`         步长（从 a 起、每 b 取一个；起点可省略，表示从该字段范围起始）
 * - `7` 在「周」字段等价于 `0`（周日）
 *
 * 设计取舍：不引入任何第三方解析库（如 dragonmantank/cron-expression），
 * 因为这个包的定位是「零重量依赖」；并且我们只在调度守护的 tick 里调用 `nextRun()`，
 * 性能完全够用（逐分钟向前扫描，5 年范围内兜底）。
 */
final class CronExpression
{
    /** @var list<int> */
    private readonly array $minutes;

    /** @var list<int> */
    private readonly array $hours;

    /** @var list<int> */
    private readonly array $daysOfMonth;

    /** @var list<int> */
    private readonly array $months;

    /** @var list<int> 已规整为 0-6（7 已映射为 0） */
    private readonly array $daysOfWeek;

    /**
     * @param list<int> $minutes
     * @param list<int> $hours
     * @param list<int> $daysOfMonth
     * @param list<int> $months
     * @param list<int> $daysOfWeek
     */
    private function __construct(
        array $minutes,
        array $hours,
        array $daysOfMonth,
        array $months,
        array $daysOfWeek,
    ) {
        $this->minutes = $minutes;
        $this->hours = $hours;
        $this->daysOfMonth = $daysOfMonth;
        $this->months = $months;
        $this->daysOfWeek = $daysOfWeek;
    }

    public static function parse(string $expression): self
    {
        $fields = preg_split('/\s+/', trim($expression));

        if ($fields === false || \count($fields) !== 5) {
            throw new CronParseException(sprintf(
                'cron 表达式必须是 5 个字段，得到 %d 个：「%s」',
                \count($fields ?? []),
                $expression,
            ));
        }

        return new self(
            self::parseField($fields[0], 0, 59, $expression),
            self::parseField($fields[1], 0, 23, $expression),
            self::parseField($fields[2], 1, 31, $expression),
            self::parseField($fields[3], 1, 12, $expression),
            self::normalizeDayOfWeek(self::parseField($fields[4], 0, 7, $expression)),
        );
    }

    /**
     * 计算严格晚于 $from 的下一次匹配时间。
     *
     * @throws CronParseException 5 年内都找不到匹配（如 2 月 30 日）
     */
    public function nextRun(DateTimeImmutable $from, ?DateTimeImmutable $upperBound = null): DateTimeImmutable
    {
        // 严格晚于 from：从下一整分钟开始扫描（秒与微秒清零）
        $t = $from->modify('+1 minute')->setTime(
            (int) $from->modify('+1 minute')->format('H'),
            (int) $from->modify('+1 minute')->format('i'),
            0,
        );
        $limit = $upperBound ?? $from->modify('+5 years');

        while ($t <= $limit) {
            if ($this->matches($t)) {
                return $t;
            }

            $t = $t->modify('+1 minute');
        }

        throw new CronParseException('在 5 年范围内没有找到匹配的 cron 运行时间');
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private function matches(DateTimeImmutable $t): bool
    {
        $minute = (int) $t->format('i');
        $hour = (int) $t->format('G');
        $dom = (int) $t->format('j');
        $month = (int) $t->format('n');
        $dow = (int) $t->format('w'); // 0=周日 .. 6=周六

        if (!\in_array($minute, $this->minutes, true)) {
            return false;
        }

        if (!\in_array($hour, $this->hours, true)) {
            return false;
        }

        if (!\in_array($month, $this->months, true)) {
            return false;
        }

        // Vixie cron 语义：日与周两个字段若都受限，则取「或」；
        // 只要有一个是 *（全量），就只约束那个受限字段。
        $domRestricted = !self::isFull($this->daysOfMonth, 1, 31);
        $dowRestricted = !self::isFull($this->daysOfWeek, 0, 6);

        if ($domRestricted && $dowRestricted) {
            return \in_array($dom, $this->daysOfMonth, true) || \in_array($dow, $this->daysOfWeek, true);
        }

        if ($domRestricted && !\in_array($dom, $this->daysOfMonth, true)) {
            return false;
        }

        return !$dowRestricted || \in_array($dow, $this->daysOfWeek, true);
    }

    /**
     * @return list<int>
     */
    private static function parseField(string $field, int $min, int $max, string $expression): array
    {
        if ($field === '*') {
            return range($min, $max);
        }

        /** @var list<int> $values */
        $values = [];

        foreach (explode(',', $field) as $part) {
            if (str_contains($part, '/')) {
                [$range, $stepRaw] = explode('/', $part, 2);
                $step = (int) $stepRaw;

                if ($step <= 0) {
                    throw new CronParseException("cron 步长必须是正整数：「{$part}」");
                }

                $base = $range === '*'
                    ? range($min, $max)
                    : self::expandRange($range, $min, $max, $expression);

                foreach ($base as $v) {
                    if (($v - $min) % $step === 0) {
                        $values[] = $v;
                    }
                }

                continue;
            }

            foreach (self::expandRange($part, $min, $max, $expression) as $v) {
                $values[] = $v;
            }
        }

        if ($values === []) {
            throw new CronParseException("cron 字段「{$field}」没有匹配到任何值");
        }

        return array_values(array_unique($values, SORT_NUMERIC));
    }

    /**
     * 把 `a-b` 区间或单个数字展开为整数列表；越界即报错。
     *
     * @return list<int>
     */
    private static function expandRange(string $part, int $min, int $max, string $expression): array
    {
        if (str_contains($part, '-')) {
            [$a, $b] = explode('-', $part, 2);
            $a = (int) $a;
            $b = (int) $b;

            if ($a > $b) {
                throw new CronParseException("cron 区间左界不能大于右界：「{$part}」");
            }

            if ($a < $min || $b > $max) {
                throw new CronParseException(sprintf(
                    'cron 区间越界：「%s」必须在 %d-%d 之间（表达式「%s」）',
                    $part,
                    $min,
                    $max,
                    $expression,
                ));
            }

            return range($a, $b);
        }

        if (!is_numeric($part) || (int) $part != $part) {
            throw new CronParseException("cron 取值非法：「{$part}」");
        }

        $v = (int) $part;

        if ($v < $min || $v > $max) {
            throw new CronParseException(sprintf(
                'cron 取值越界：「%s」必须在 %d-%d 之间',
                $part,
                $min,
                $max,
            ));
        }

        return [$v];
    }

    /**
     * 周字段把 7 归并为 0（周日），并去重，得到规范化的 0-6 集合。
     *
     * @param list<int> $values
     * @return list<int>
     */
    private static function normalizeDayOfWeek(array $values): array
    {
        $normalized = array_map(static fn (int $v): int => $v === 7 ? 0 : $v, $values);

        return array_values(array_unique($normalized, SORT_NUMERIC));
    }

    /**
     * 列表是否正好是 [min, min+1, ..., max] 的全集（用于判断该字段是否为 *）。
     *
     * @param list<int> $values
     */
    private static function isFull(array $values, int $min, int $max): bool
    {
        return $values === range($min, $max);
    }
}
