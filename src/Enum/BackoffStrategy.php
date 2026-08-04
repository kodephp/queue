<?php

declare(strict_types=1);

namespace Kode\Queue\Enum;

use Random\Randomizer;

/**
 * 重试退避策略。
 *
 * 所有计算集中在枚举内部，Worker 与中间件只需声明策略，不再各自实现一遍退避公式。
 */
enum BackoffStrategy: string
{
    /** 固定间隔 */
    case Fixed = 'fixed';

    /** 线性递增：base * n */
    case Linear = 'linear';

    /** 指数递增：base * 2^(n-1) */
    case Exponential = 'exponential';

    /** 指数递增 + 全抖动，避免重试风暴 */
    case ExponentialJitter = 'exponential_jitter';

    /** 斐波那契递增，比指数更平缓 */
    case Fibonacci = 'fibonacci';

    /** 退避上限默认值（秒） */
    public const int DEFAULT_CAP = 3600;

    /**
     * 计算第 N 次重试应等待的秒数。
     *
     * @param int<1, max> $attempt 第几次重试（从 1 开始）
     * @param int         $base    基础间隔（秒）
     * @param int         $cap     上限（秒），0 表示不封顶
     */
    public function delayFor(int $attempt, int $base = 1, int $cap = self::DEFAULT_CAP): int
    {
        $attempt = max(1, $attempt);
        $base = max(0, $base);

        $delay = match ($this) {
            self::Fixed => $base,
            self::Linear => $base * $attempt,
            self::Exponential => (int) ($base * 2 ** ($attempt - 1)),
            self::ExponentialJitter => self::jitter((int) ($base * 2 ** ($attempt - 1))),
            self::Fibonacci => $base * self::fibonacci($attempt),
        };

        return $cap > 0 ? min($delay, $cap) : $delay;
    }

    /**
     * 生成一条完整的退避序列，便于配置校验与文档展示。
     *
     * @return list<int>
     */
    public function sequence(int $attempts, int $base = 1, int $cap = self::DEFAULT_CAP): array
    {
        return array_map(
            fn (int $n): int => $this->delayFor($n, $base, $cap),
            range(1, max(1, $attempts)),
        );
    }

    /** 全抖动：在 [0, delay] 之间随机取值 */
    private static function jitter(int $delay): int
    {
        if ($delay <= 0) {
            return 0;
        }

        // PHP 8.2+ 的 Randomizer，比 rand() 更快且可注入引擎、便于测试复现
        static $randomizer = new Randomizer();

        return $randomizer->getInt(0, $delay);
    }

    private static function fibonacci(int $n): int
    {
        [$a, $b] = [1, 1];
        for ($i = 2; $i <= $n; $i++) {
            [$a, $b] = [$b, $a + $b];
        }

        return $a;
    }
}
