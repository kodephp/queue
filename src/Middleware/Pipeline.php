<?php

declare(strict_types=1);

namespace Kode\Queue\Middleware;

use Kode\Queue\Contract\JobMiddlewareInterface;
use Kode\Queue\Contract\MiddlewareInterface;
use Kode\Queue\Message\ReservedJob;

/**
 * 中间件管道。
 *
 * 相比 v1 在 `AbstractQueue::dispatch()` 里就地拼闭包的做法：
 * - 管道本身不可变，可安全缓存与复用；
 * - 支持按 `priority()` 稳定排序，中间件顺序不再取决于 `addMiddleware()` 的调用次序；
 * - 编译结果（嵌套闭包）被缓存，热路径上不用每次重建 N 层闭包。
 *
 * @template TSubject of Operation|ReservedJob
 */
final class Pipeline
{
    /** @var list<MiddlewareInterface|JobMiddlewareInterface> */
    private array $middleware;

    /** @var (callable(TSubject): mixed)|null 编译后的调用链缓存 */
    private $compiled = null;

    /** @var (callable(TSubject): mixed)|null 编译时使用的终点 */
    private $compiledFor = null;

    /**
     * @param iterable<MiddlewareInterface|JobMiddlewareInterface> $middleware
     */
    public function __construct(iterable $middleware = [])
    {
        $this->middleware = self::sort([...$middleware]);
    }

    /**
     * 追加中间件，返回新管道（原管道不变）。
     *
     * @return Pipeline<Operation|ReservedJob>
     */
    public function with(MiddlewareInterface|JobMiddlewareInterface ...$middleware): self
    {
        return new self([...$this->middleware, ...$middleware]);
    }

    /**
     * 移除指定类型的中间件，返回新管道。
     *
     * @param  class-string ...$classes
     * @return Pipeline<Operation|ReservedJob>
     */
    public function without(string ...$classes): self
    {
        return new self(array_filter(
            $this->middleware,
            static fn (object $m): bool => !\in_array($m::class, $classes, true),
        ));
    }

    /**
     * 执行管道。
     *
     * @param TSubject                   $subject
     * @param callable(TSubject): mixed  $destination 管道终点（真正的驱动调用）
     */
    public function run(object $subject, callable $destination): mixed
    {
        if ($this->middleware === []) {
            return $destination($subject);
        }

        // 终点相同则复用已编译的链，避免热路径重复构建闭包
        if ($this->compiled === null || $this->compiledFor !== $destination) {
            $this->compiled = $this->compile($destination);
            $this->compiledFor = $destination;
        }

        return ($this->compiled)($subject);
    }

    /**
     * @return list<MiddlewareInterface|JobMiddlewareInterface>
     */
    public function all(): array
    {
        return $this->middleware;
    }

    public function isEmpty(): bool
    {
        return $this->middleware === [];
    }

    public function count(): int
    {
        return \count($this->middleware);
    }

    /**
     * 把中间件列表折叠成一条嵌套闭包。
     *
     * @param  callable(TSubject): mixed $destination
     * @return callable(TSubject): mixed
     */
    private function compile(callable $destination): callable
    {
        return array_reduce(
            array_reverse($this->middleware),
            static fn (callable $next, object $middleware): callable
                => static fn (object $subject): mixed => $middleware->handle($subject, $next),
            $destination,
        );
    }

    /**
     * 按优先级降序稳定排序（相同优先级保持注册顺序）。
     *
     * @param  list<MiddlewareInterface|JobMiddlewareInterface> $middleware
     * @return list<MiddlewareInterface|JobMiddlewareInterface>
     */
    private static function sort(array $middleware): array
    {
        $indexed = [];
        foreach ($middleware as $index => $item) {
            $indexed[] = [$item->priority(), -$index, $item];
        }

        usort($indexed, static fn (array $a, array $b): int => [$b[0], $b[1]] <=> [$a[0], $a[1]]);

        return array_column($indexed, 2);
    }
}
