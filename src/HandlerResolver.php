<?php

declare(strict_types=1);

namespace Kode\Queue;

use Kode\Queue\Attribute\AsJob;
use Kode\Queue\Contract\HandlerResolverInterface;
use Kode\Queue\Exception\JobNotFoundException;
use Kode\Queue\Message\Job;
use Override;
use Psr\Container\ContainerInterface;

/**
 * 默认任务处理器解析器。
 *
 * 按以下顺序尝试把「任务名」变成「可执行代码」，第一个命中即返回：
 *
 * 1. **显式注册**：`register('send.mail', fn (Job $job) => ...)`，优先级最高，测试里最好用；
 * 2. **别名表**：`#[AsJob(name: 'send.mail')]` 注册过的类；
 * 3. **PSR-11 容器**：装了 **kode/di** 或任何 PSR-11 容器时，交给容器做依赖注入；
 * 4. **直接实例化**：任务名本身是个可自动构造的类，反射 new 出来。
 *
 * 找到实例后，依次尝试 `handle()`、`__invoke()`、`run()` 三个约定方法，
 * 并按方法签名自动决定传 `Job` 还是裸 payload 数组——
 * 这样既支持「面向对象任务类」，也支持「一个闭包搞定」的轻量写法。
 */
final class HandlerResolver implements HandlerResolverInterface
{
    /** 依次尝试的处理方法名 */
    private const array HANDLE_METHODS = ['handle', '__invoke', 'run', 'execute'];

    /** @var array<string, callable|class-string> */
    private array $handlers = [];

    /** @var array<string, callable(Job): mixed> 解析结果缓存 */
    private array $resolved = [];

    public function __construct(
        private readonly ?ContainerInterface $container = null,
    ) {
    }

    /**
     * 注册处理器。
     *
     * @param callable|class-string $handler
     */
    public function register(string $name, callable|string $handler): self
    {
        $this->handlers[$name] = $handler;
        unset($this->resolved[$name]);

        return $this;
    }

    /**
     * 批量注册。
     *
     * 键值对形态是「任务名 => 处理器」；如果键是数字下标（即写成列表），
     * 则值必须是任务类名，按 {@see registerJobClass()} 处理 —— 任务名从 `#[AsJob]` 读，
     * 于是下面两种写法可以混着写：
     *
     * ```php
     * $resolver->registerMany([
     *     'mail.send'          => $closure,   // 名称 => 处理器
     *     SendWelcomeMail::class,             // 纯类名，名字由属性决定
     * ]);
     * ```
     *
     * @param array<array-key, callable|class-string> $handlers
     */
    public function registerMany(array $handlers): self
    {
        foreach ($handlers as $name => $handler) {
            if (\is_int($name)) {
                if (!\is_string($handler) || !class_exists($handler)) {
                    throw new \InvalidArgumentException(
                        '列表形式的处理器条目必须是存在的任务类名，'
                        . '若要注册闭包请使用「任务名 => 闭包」的写法',
                    );
                }

                $this->registerJobClass($handler);

                continue;
            }

            $this->register($name, $handler);
        }

        return $this;
    }

    /**
     * 注册一个带 `#[AsJob]` 的任务类，自动用属性里声明的名字建立别名。
     *
     * @param class-string $class
     */
    public function registerJobClass(string $class): self
    {
        $this->register(AsJob::nameOf($class), $class);

        if (!isset($this->handlers[$class])) {
            $this->register($class, $class);
        }

        return $this;
    }

    /**
     * 批量注册任务类（可配合 kode/attributes 的扫描结果使用）。
     *
     * @param iterable<class-string> $classes
     */
    public function discover(iterable $classes): self
    {
        foreach ($classes as $class) {
            if (AsJob::of($class) !== null) {
                $this->registerJobClass($class);
            }
        }

        return $this;
    }

    /**
     * 已注册的任务名列表。
     *
     * @return list<string>
     */
    public function registered(): array
    {
        return array_keys($this->handlers);
    }

    #[Override]
    public function canResolve(Job $job): bool
    {
        if (isset($this->handlers[$job->name]) || isset($this->resolved[$job->name])) {
            return true;
        }

        if ($this->container?->has($job->name) === true) {
            return true;
        }

        return class_exists($job->name);
    }

    #[Override]
    public function resolve(Job $job): callable
    {
        if (isset($this->resolved[$job->name])) {
            return $this->resolved[$job->name];
        }

        $target = $this->handlers[$job->name] ?? $job->name;

        // 已经是闭包 / 可调用体：直接用
        if (!\is_string($target) && \is_callable($target)) {
            return $this->resolved[$job->name] = $this->adapt($target);
        }

        $instance = $this->instantiate((string) $target, $job);
        $method = $this->pickMethod($instance);

        return $this->resolved[$job->name] = $this->adapt([$instance, $method]);
    }

    /**
     * 清空解析缓存（热重载 / 单元测试之间隔离用）。
     */
    public function forget(): void
    {
        $this->resolved = [];
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private function instantiate(string $target, Job $job): object
    {
        // 容器优先：能拿到依赖注入好的实例
        if ($this->container?->has($target) === true) {
            $instance = $this->container->get($target);

            if (\is_object($instance)) {
                return $instance;
            }
        }

        if (!class_exists($target)) {
            throw JobNotFoundException::handler($job->name);
        }

        $reflection = new \ReflectionClass($target);

        if (!$reflection->isInstantiable()) {
            throw JobNotFoundException::handler($job->name, '类不可实例化（抽象类 / 接口 / 私有构造函数）');
        }

        $constructor = $reflection->getConstructor();

        // 无构造参数（或全部有默认值）：直接 new
        if ($constructor === null || $constructor->getNumberOfRequiredParameters() === 0) {
            return $reflection->newInstance();
        }

        // 有必填依赖但没有容器：按参数类型逐个从容器取，取不到就报错
        return $reflection->newInstanceArgs($this->buildArguments($constructor, $job));
    }

    /**
     * @return list<mixed>
     */
    private function buildArguments(\ReflectionMethod $constructor, Job $job): array
    {
        $arguments = [];

        foreach ($constructor->getParameters() as $parameter) {
            $type = $parameter->getType();
            $name = $type instanceof \ReflectionNamedType && !$type->isBuiltin() ? $type->getName() : null;

            $arguments[] = match (true) {
                $name === Job::class => $job,
                $name !== null && $this->container?->has($name) === true => $this->container->get($name),
                $parameter->isDefaultValueAvailable() => $parameter->getDefaultValue(),
                $parameter->allowsNull() => null,
                default => throw JobNotFoundException::handler(
                    $job->name,
                    sprintf('构造参数 $%s 无法自动注入，请注册 PSR-11 容器或改用闭包处理器', $parameter->getName()),
                ),
            };
        }

        return $arguments;
    }

    private function pickMethod(object $instance): string
    {
        foreach (self::HANDLE_METHODS as $method) {
            if (method_exists($instance, $method)) {
                return $method;
            }
        }

        throw JobNotFoundException::handler(
            $instance::class,
            sprintf('缺少 %s 中任意一个处理方法', implode(' / ', self::HANDLE_METHODS)),
        );
    }

    /**
     * 统一处理器签名：
     * 声明了 `Job` 参数就把整个任务对象传进去，否则只传 payload 数组。
     *
     * @param  callable            $handler
     * @return callable(Job): mixed
     */
    private function adapt(callable $handler): callable
    {
        $wantsJob = $this->expectsJob($handler);

        return static fn (Job $job): mixed => $wantsJob
            ? $handler($job)
            : $handler($job->payload, $job);
    }

    private function expectsJob(callable $handler): bool
    {
        try {
            $reflection = \is_array($handler)
                ? new \ReflectionMethod($handler[0], (string) $handler[1])
                : new \ReflectionFunction($handler(...));

            $first = $reflection->getParameters()[0] ?? null;
            $type = $first?->getType();

            if ($type instanceof \ReflectionNamedType) {
                return $type->getName() === Job::class;
            }

            // 无类型声明时按「只要 payload」处理，兼容 v1 的 fn (array $data) 写法
            return false;
        } catch (\ReflectionException) {
            return false;
        }
    }
}
