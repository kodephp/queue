<?php

declare(strict_types=1);

namespace Kode\Queue\Attribute;

use Attribute;
use Kode\Queue\Enum\BackoffStrategy;
use Kode\Queue\Enum\Priority;

/**
 * 把一个类标记为队列任务，并声明它的默认投递参数。
 *
 * v1 需要在每个投递点手写 `['queue' => 'mail', 'max_attempts' => 5, ...]`，
 * 同一个任务在十个地方投递就有十份可能不一致的配置。
 * 用属性把这些参数绑在任务类上，投递方只管投，参数由任务自己说了算：
 *
 * ```php
 * #[AsJob(queue: 'mail', maxAttempts: 5, priority: Priority::High, timeout: 120)]
 * final class SendWelcomeMail
 * {
 *     public function handle(array $payload): void { ... }
 * }
 *
 * $queue->push(SendWelcomeMail::class, ['user_id' => 42]);
 * ```
 *
 * 与 **kode/attributes** 的扫描器配合时，可在启动阶段一次性收集全部任务类，
 * 无需手工注册；未安装该包也不影响使用，解析器会在投递时按需读取属性。
 */
#[Attribute(Attribute::TARGET_CLASS)]
final readonly class AsJob
{
    /**
     * @param string|null          $name        任务名，默认取类名
     * @param string|null          $queue       默认队列
     * @param int|null             $maxAttempts 最大尝试次数
     * @param Priority             $priority    优先级
     * @param int|null             $timeout     单次执行超时（秒）
     * @param BackoffStrategy      $backoff     退避策略
     * @param int                  $backoffBase 退避基数（秒）
     * @param bool                 $unique      是否幂等（配合 IdempotencyMiddleware 去重）
     * @param array<string, mixed> $headers     默认附加头
     */
    public function __construct(
        public ?string $name = null,
        public ?string $queue = null,
        public ?int $maxAttempts = null,
        public Priority $priority = Priority::Normal,
        public ?int $timeout = null,
        public BackoffStrategy $backoff = BackoffStrategy::ExponentialJitter,
        public int $backoffBase = 5,
        public bool $unique = false,
        public array $headers = [],
    ) {
    }

    /**
     * 读取某个类上的 #[AsJob]，未标注时返回 null。
     *
     * @param class-string|object $target
     */
    public static function of(string|object $target): ?self
    {
        $class = \is_object($target) ? $target::class : $target;

        if (!class_exists($class)) {
            return null;
        }

        $attributes = (new \ReflectionClass($class))->getAttributes(self::class);

        return $attributes === [] ? null : $attributes[0]->newInstance();
    }

    /**
     * 解析出最终任务名。
     *
     * @param class-string|object $target
     */
    public static function nameOf(string|object $target): string
    {
        $class = \is_object($target) ? $target::class : $target;

        return self::of($class)?->name ?? $class;
    }
}
