<?php

declare(strict_types=1);

namespace Kode\Queue\Console;

use Kode\Queue\Config\WorkerOptions;
use Kode\Queue\Contract\FailedJobStoreInterface;
use Kode\Queue\Failed\FileFailedJobStore;
use Kode\Queue\HandlerResolver;
use Kode\Queue\QueueManager;
use Kode\Queue\Scheduler;
use Kode\Queue\Worker;

/**
 * 零依赖命令行入口。
 *
 * v1 只提供库，没有 runner——用户要跑 Worker 得自己写一个 `worker.php`，
 * 于是信号处理、内存回收、失败重放这些又得各写一遍。
 *
 * 这里刻意**不引入 symfony/console**：一个队列组件不该为了打印几行帮助
 * 就把整棵依赖树拖进用户项目。参数解析、着色输出、表格全部手写，总共两百来行。
 *
 * 用法：
 *
 * ```bash
 * vendor/bin/kode-queue work --bootstrap=queue.php --queue=high,default
 * vendor/bin/kode-queue failed:list --bootstrap=queue.php
 * vendor/bin/kode-queue failed:retry all --bootstrap=queue.php
 * ```
 *
 * `--bootstrap` 指向一个 PHP 文件，返回值可以是：
 *
 * - {@see QueueManager} 实例（只能跑 size / stats / diagnose，没有处理器）；
 * - 数组 `['default' => ..., 'connections' => [...], 'handlers' => [...], 'failed' => ...]`；
 * - 数组 `['manager' => $manager, 'handlers' => [...], 'failed' => ...]`
 *   —— 自己从容器里取 QueueManager，同时仍能注册处理器与死信存储。
 *
 * 其中 `handlers` 是「任务名 => 闭包 / 类名」，`failed` 是
 * {@see FailedJobStoreInterface} 实例或一个目录路径（自动用文件存储）。
 */
final class Application
{
    public const string VERSION = '3.0.10';

    /** @var array<string, string> 命令 => 一句话说明 */
    private const array COMMANDS = [
        'work' => '启动 Worker 消费队列',
        'size' => '查看队列积压数量',
        'stats' => '查看队列各状态统计',
        'diagnose' => '诊断各连接的可用性与能力',
        'failed:list' => '列出失败任务',
        'failed:show' => '查看某条失败任务的完整异常',
        'failed:retry' => '重放失败任务（传 ID 或 all）',
        'failed:forget' => '删除某条失败任务',
        'failed:flush' => '清空失败任务',
        'flush' => '清空实时队列（clear）',
        'benchmark' => '队列吞吐压测（--jobs=N --consume）',
        'schedule' => '运行定时任务守护（--schedule=FILE）',
    ];

    private QueueManager $manager;

    /** @var array<string, callable|class-string> */
    private array $handlers = [];

    private ?FailedJobStoreInterface $failedStore = null;

    /** @param resource $stream */
    public function __construct(private $stream = \STDOUT)
    {
    }

    /**
     * @param  list<string> $argv
     * @return int          进程退出码
     */
    public function run(array $argv): int
    {
        $command = $argv[1] ?? 'help';
        $options = self::parseOptions(\array_slice($argv, 2));
        $arguments = self::parseArguments(\array_slice($argv, 2));

        if (\in_array($command, ['help', '--help', '-h', ''], true)) {
            $this->printHelp();

            return 0;
        }

        if (\in_array($command, ['--version', '-V'], true)) {
            $this->line('kode/queue ' . self::VERSION);

            return 0;
        }

        if (!isset(self::COMMANDS[$command])) {
            $this->error(sprintf('未知命令 "%s"', $command));
            $this->printHelp();

            return 1;
        }

        try {
            $this->bootstrap($options);

            return match ($command) {
                'work' => $this->work($options),
                'size' => $this->size($options),
                'stats' => $this->stats($options),
                'diagnose' => $this->diagnose(),
                'failed:list' => $this->failedList($options),
                'failed:show' => $this->failedShow($arguments),
                'failed:retry' => $this->failedRetry($arguments, $options),
                'failed:forget' => $this->failedForget($arguments),
                'failed:flush' => $this->failedFlush($options),
                'flush' => $this->flush($options),
                'benchmark' => $this->benchmark($options),
                'schedule' => $this->schedule($options),
                default => 1,
            };
        } catch (\Throwable $e) {
            $this->error($e->getMessage());

            if (isset($options['v']) || isset($options['verbose'])) {
                $this->line($e->getTraceAsString());
            }

            return 1;
        }
    }

    // ------------------------------------------------------------------
    // 命令实现
    // ------------------------------------------------------------------

    /** @param array<string, string|bool> $options */
    private function work(array $options): int
    {
        $queue = $this->connection($options);
        $worker = new Worker(
            queue: $queue,
            resolver: (new HandlerResolver())->registerMany($this->handlers),
            options: $this->workerOptions($options, $queue->getDefaultQueue()),
            failedStore: $this->failedStore,
            logger: new StdoutLogger($this->stream, isset($options['quiet'])),
        );

        if (isset($options['once'])) {
            $worker->runOnce();

            return 0;
        }

        $summary = $worker->run();

        $this->line('');
        $this->line((string) $summary);

        return $summary->isClean() ? 0 : 1;
    }

    /** @param array<string, string|bool> $options */
    private function size(array $options): int
    {
        $queue = $this->connection($options);

        foreach ($this->queueNames($options, $queue->getDefaultQueue()) as $name) {
            $this->line(self::pad($name, 24) . ' ' . $queue->size($name));
        }

        return 0;
    }

    /** @param array<string, string|bool> $options */
    private function stats(array $options): int
    {
        $queue = $this->connection($options);

        foreach ($this->queueNames($options, $queue->getDefaultQueue()) as $name) {
            $stats = $queue->stats($name);
            $this->line(self::pad($name, 20) . sprintf(
                ' 就绪 %-8d 延迟 %-8d 预留 %-8d 合计 %d',
                $stats->ready,
                $stats->delayed,
                $stats->reserved,
                $stats->total,
            ));
        }

        return 0;
    }

    private function diagnose(): int
    {
        foreach ($this->manager->diagnose() as $name => $report) {
            $available = ($report['available'] ?? false) === true;
            $this->line(
                ($available ? '✓' : '✗')
                . ' ' . self::pad((string) $name, 14)
                . ' driver=' . self::pad((string) ($report['driver'] ?? '?'), 12)
                . ' ' . ($available
                    ? '能力：' . implode(', ', (array) ($report['capabilities'] ?? []))
                    : (string) ($report['install_hint'] ?? '不可用')),
            );
        }

        return 0;
    }

    /** @param array<string, string|bool> $options */
    private function failedList(array $options): int
    {
        $store = $this->requireFailedStore();
        $queue = \is_string($options['queue'] ?? null) ? $options['queue'] : null;
        $records = $store->all($queue, (int) ($options['limit'] ?? 50));

        if ($records === []) {
            $this->line('没有失败任务。');

            return 0;
        }

        $this->line(
            self::pad('ID', 28) . ' ' . self::pad('队列', 16)
            . ' ' . self::pad('任务', 24) . ' 失败时间',
        );

        foreach ($records as $record) {
            $this->line(
                self::pad($record['id'], 28)
                . ' ' . self::pad($record['queue'], 16)
                . ' ' . self::pad(self::truncate($record['job']->name, 24), 24)
                . ' ' . date('Y-m-d H:i:s', $record['failed_at']),
            );
        }

        $this->line(sprintf('共 %d 条（总计 %d）', \count($records), $store->count($queue)));

        return 0;
    }

    /** @param list<string> $arguments */
    private function failedShow(array $arguments): int
    {
        $record = $this->requireFailedStore()->find($arguments[0] ?? '');

        if ($record === null) {
            $this->error('未找到该失败任务');

            return 1;
        }

        $this->line('ID       ' . $record['id']);
        $this->line('连接     ' . $record['connection']);
        $this->line('队列     ' . $record['queue']);
        $this->line('任务     ' . (string) $record['job']);
        $this->line('尝试     ' . $record['job']->attempts . ' / ' . $record['job']->maxAttempts);
        $this->line('时间     ' . date('Y-m-d H:i:s', $record['failed_at']));
        $this->line('负载     ' . json_encode($record['job']->payload, \JSON_UNESCAPED_UNICODE));
        $this->line('');
        $this->line($record['exception']);

        return 0;
    }

    /**
     * @param list<string>               $arguments
     * @param array<string, string|bool> $options
     */
    private function failedRetry(array $arguments, array $options): int
    {
        $store = $this->requireFailedStore();
        $target = $arguments[0] ?? '';

        if ($target === '') {
            $this->error('请指定失败任务 ID，或用 all 重放全部');

            return 1;
        }

        $records = $target === 'all'
            ? $store->all(\is_string($options['queue'] ?? null) ? $options['queue'] : null, 10000)
            : array_filter([$store->find($target)]);

        if ($records === []) {
            $this->error('没有可重放的任务');

            return 1;
        }

        $replayed = 0;

        foreach ($records as $record) {
            // 重置尝试次数，否则一进队列就又判定为「已耗尽」
            $job = $record['job']->resetAttempts()->withStatus(\Kode\Queue\Enum\JobStatus::Pending);
            $this->manager->connection($record['connection'])->pushJob($job);
            $store->forget($record['id']);
            $replayed++;
        }

        $this->line(sprintf('已重放 %d 个任务。', $replayed));

        return 0;
    }

    /** @param list<string> $arguments */
    private function failedForget(array $arguments): int
    {
        $ok = $this->requireFailedStore()->forget($arguments[0] ?? '');
        $this->line($ok ? '已删除。' : '未找到该失败任务。');

        return $ok ? 0 : 1;
    }

    /** @param array<string, string|bool> $options */
    private function failedFlush(array $options): int
    {
        $olderThan = isset($options['days'])
            ? time() - ((int) $options['days'] * 86400)
            : null;

        $removed = $this->requireFailedStore()->flush(
            \is_string($options['queue'] ?? null) ? $options['queue'] : null,
            $olderThan,
        );

        $this->line(sprintf('已清理 %d 条失败任务。', $removed));

        return 0;
    }

    /** @param array<string, string|bool> $options */
    private function flush(array $options): int
    {
        $queue = $this->connection($options);
        $name = \is_string($options['queue'] ?? null) ? $options['queue'] : $queue->getDefaultQueue();
        $removed = $queue->clear($name);

        $this->line(sprintf('已清空队列「%s」，移除 %d 条任务。', $name, $removed));

        return 0;
    }

    /**
     * 队列吞吐压测：先批量投递 N 个任务测「入队」速率，
     * 加 `--consume` 再启动一个内置空处理器把这批任务消费掉，测「消费」速率。
     *
     * @param array<string, string|bool> $options
     */
    private function benchmark(array $options): int
    {
        $queue = $this->connection($options);
        $name = \is_string($options['queue'] ?? null) ? $options['queue'] : $queue->getDefaultQueue();
        $jobs = max(1, (int) ($options['jobs'] ?? 10000));
        $jobName = 'kode:benchmark';

        // 入队阶段：用 bulk 一次性投递，更贴近真实批量场景
        $t0 = microtime(true);
        $queue->onQueue($name)->bulk(array_fill(0, $jobs, $jobName));
        $enqueueSec = microtime(true) - $t0;
        $enqueueRate = $enqueueSec > 0.0 ? $jobs / $enqueueSec : 0.0;
        $this->line(sprintf(
            '入队   : %d 个任务，耗时 %.3fs，吞吐 %.0f jobs/s',
            $jobs,
            $enqueueSec,
            $enqueueRate,
        ));

        if (!isset($options['consume'])) {
            return 0;
        }

        // 消费阶段：注册一个空处理器把这批任务跑完（stopWhenEmpty 跑空即退）
        $worker = new Worker(
            queue: $queue,
            resolver: (new HandlerResolver())->registerMany([$jobName => static fn (): null => null]),
            options: WorkerOptions::batch($name),
            logger: new StdoutLogger($this->stream, true),
        );

        $t1 = microtime(true);
        $summary = $worker->run();
        $consumeSec = microtime(true) - $t1;
        $consumeRate = $consumeSec > 0.0 ? $summary->processed / $consumeSec : 0.0;

        $this->line(sprintf(
            '消费   : %d 个任务，耗时 %.3fs，吞吐 %.0f jobs/s',
            $summary->processed,
            $consumeSec,
            $consumeRate,
        ));
        $this->line(sprintf('结果   : 成功 %d / 失败 %d', $summary->succeeded(), $summary->failed));

        return $summary->isClean() ? 0 : 1;
    }

    /**
     * 运行定时任务守护：加载调度定义文件，按 `--interval` 节奏把到期任务投递进队列。
     *
     * @param array<string, string|bool> $options
     */
    private function schedule(array $options): int
    {
        $path = $options['schedule'] ?? null;

        if (!\is_string($path) || !is_file($path)) {
            $this->error('请通过 --schedule=FILE 指定调度定义文件');
            $this->line('示例：return \\Kode\\Queue\\Scheduler::for($queue)->cron("0 9 * * *", App\\Report::class);');

            return 1;
        }

        /** @var mixed $loaded */
        $loaded = require $path;
        $scheduler = $this->resolveScheduler($loaded, $options);

        $interval = (float) ($options['interval'] ?? 60.0);

        if (isset($options['once'])) {
            $count = $scheduler->tick($interval);
            $this->line(sprintf('本次（单次）投递 %d 个定时任务。', $count));

            return 0;
        }

        $this->line(sprintf('调度守护启动，每 %.0fs 检查一次（Ctrl+C 退出）', $interval));

        $scheduler->run($interval);

        return 0;
    }

    /**
     * 把调度定义文件返回值规整为 {@see Scheduler}。
     *
     * 支持三种返回形态：
     *
     * - {@see Scheduler} 实例（最推荐，直接持有自己的队列连接）
     * - `['scheduler' => Scheduler]` 或 `['schedules' => [...]]`
     * - 调度定义列表：`[['cron' => '0 9 * * *', 'job' => ..., 'data' => [...], 'queue' => ...], ...]`
     *
     * @param array<string, string|bool> $options
     */
    private function resolveScheduler(mixed $loaded, array $options): Scheduler
    {
        if ($loaded instanceof Scheduler) {
            return $loaded;
        }

        if (!\is_array($loaded)) {
            throw new \RuntimeException('调度文件必须返回 Scheduler 实例或定义数组');
        }

        $scheduler = $loaded['scheduler'] ?? null;
        if ($scheduler instanceof Scheduler) {
            return $scheduler;
        }

        $defs = $loaded['schedules'] ?? $loaded;
        if ($defs instanceof Scheduler) {
            return $defs;
        }

        $queue = $this->connection($options);
        $scheduler = Scheduler::for($queue);

        foreach ($defs as $def) {
            if (!\is_array($def) || !isset($def['cron'], $def['job'])) {
                throw new \RuntimeException('调度定义必须包含 cron 与 job 键');
            }

            $scheduler->cron(
                $def['cron'],
                $def['job'],
                (array) ($def['data'] ?? []),
                $def['queue'] ?? null,
            );
        }

        return $scheduler;
    }

    // ------------------------------------------------------------------
    // 引导与配置
    // ------------------------------------------------------------------

    /** @param array<string, string|bool> $options */
    private function bootstrap(array $options): void
    {
        $path = $options['bootstrap'] ?? $options['config'] ?? null;

        if (!\is_string($path)) {
            $this->manager = QueueManager::fromEnv();

            return;
        }

        if (!is_file($path)) {
            throw new \RuntimeException(sprintf('引导文件不存在：%s', $path));
        }

        /** @var mixed $loaded */
        $loaded = require $path;

        if ($loaded instanceof QueueManager) {
            $this->manager = $loaded;

            return;
        }

        if (!\is_array($loaded)) {
            throw new \RuntimeException('引导文件必须返回 QueueManager 实例或配置数组');
        }

        /** @var array<string, mixed> $loaded */
        $this->handlers = (array) ($loaded['handlers'] ?? []);
        $this->failedStore = self::resolveFailedStore($loaded['failed'] ?? null);

        $manager = $loaded['manager'] ?? null;

        unset($loaded['handlers'], $loaded['failed'], $loaded['manager']);

        // 允许 `['manager' => $manager, 'handlers' => [...]]`：
        // 调用方自己组装好 QueueManager（比如从容器里取），仍然能注册处理器与死信存储。
        $this->manager = $manager instanceof QueueManager
            ? $manager
            : QueueManager::make($loaded);
    }

    private static function resolveFailedStore(mixed $value): ?FailedJobStoreInterface
    {
        return match (true) {
            $value instanceof FailedJobStoreInterface => $value,
            \is_string($value) && $value !== '' => new FileFailedJobStore($value),
            default => null,
        };
    }

    private function requireFailedStore(): FailedJobStoreInterface
    {
        return $this->failedStore
            ?? throw new \RuntimeException('未配置失败任务存储，请在引导文件里设置 failed 项');
    }

    /** @param array<string, string|bool> $options */
    private function connection(array $options): \Kode\Queue\Queue
    {
        return $this->manager->connection(
            \is_string($options['connection'] ?? null) ? $options['connection'] : null,
        );
    }

    /**
     * @param  array<string, string|bool> $options
     * @return list<string>
     */
    private function queueNames(array $options, string $default): array
    {
        $raw = $options['queue'] ?? null;

        if (!\is_string($raw) || $raw === '') {
            return [$default];
        }

        return array_values(array_filter(array_map(trim(...), explode(',', $raw))));
    }

    /** @param array<string, string|bool> $options */
    private function workerOptions(array $options, string $default): WorkerOptions
    {
        return WorkerOptions::fromArray([
            'name' => $options['name'] ?? ('kode-worker@' . gethostname()),
            'queues' => $this->queueNames($options, $default),
            'sleep' => $options['sleep'] ?? 1.0,
            'block_timeout' => $options['block'] ?? 5.0,
            'max_jobs' => $options['max-jobs'] ?? 0,
            'max_time' => $options['max-time'] ?? 0,
            'memory_limit' => $options['memory'] ?? 128,
            'max_attempts' => $options['tries'] ?? 0,
            'backoff_base' => $options['backoff'] ?? 0,
            'stop_when_empty' => isset($options['stop-when-empty']),
            'reclaim_every' => $options['reclaim'] ?? 60.0,
        ]);
    }

    // ------------------------------------------------------------------
    // 参数解析与输出
    // ------------------------------------------------------------------

    /**
     * @param  list<string>               $argv
     * @return array<string, string|bool>
     */
    private static function parseOptions(array $argv): array
    {
        $options = [];

        foreach ($argv as $token) {
            if (!str_starts_with($token, '-')) {
                continue;
            }

            $token = ltrim($token, '-');
            [$key, $value] = str_contains($token, '=')
                ? explode('=', $token, 2)
                : [$token, true];

            $options[$key] = $value;
        }

        return $options;
    }

    /**
     * @param  list<string> $argv
     * @return list<string>
     */
    private static function parseArguments(array $argv): array
    {
        return array_values(array_filter($argv, static fn (string $t): bool => !str_starts_with($t, '-')));
    }

    private function printHelp(): void
    {
        $this->line('kode/queue ' . self::VERSION . ' — 队列命令行工具');
        $this->line('');
        $this->line('用法：');
        $this->line('  kode-queue <命令> [参数] [选项]');
        $this->line('');
        $this->line('命令：');

        foreach (self::COMMANDS as $name => $description) {
            $this->line(sprintf('  %-16s %s', $name, $description));
        }

        $this->line('');
        $this->line('通用选项：');
        $this->line('  --bootstrap=FILE  引导文件（返回 QueueManager 或配置数组）');
        $this->line('  --connection=NAME 指定连接，默认用配置里的 default');
        $this->line('  --queue=A,B       队列名，逗号分隔即优先级顺序');
        $this->line('');
        $this->line('work 选项：');
        $this->line('  --once            只处理一个任务后退出');
        $this->line('  --stop-when-empty 队列跑空即退出（CI / 补数场景）');
        $this->line('  --max-jobs=N      处理 N 个任务后退出');
        $this->line('  --max-time=N      运行 N 秒后退出');
        $this->line('  --memory=N        内存超过 N MB 后退出（默认 128）');
        $this->line('  --tries=N         全局重试上限（0 = 由任务自己声明）');
        $this->line('  --sleep=N         队列为空时休眠秒数');
        $this->line('  --quiet           只输出错误');
        $this->line('');
        $this->line('benchmark 选项：');
        $this->line('  --jobs=N      投递并（可选）消费的任务数，默认 10000');
        $this->line('  --consume     压测入队后，再启动 Worker 把这批任务消费掉');
        $this->line('');
        $this->line('schedule 选项：');
        $this->line('  --schedule=FILE  调度定义文件（返回 Scheduler 实例或定义数组）');
        $this->line('  --interval=N     检查间隔秒数，默认 60');
        $this->line('  --once           只检查并投递一轮就退出（dry-run）');
    }

    private static function truncate(string $value, int $length): string
    {
        return mb_strlen($value) <= $length ? $value : mb_substr($value, 0, $length - 1) . '…';
    }

    /**
     * 按**终端显示宽度**右侧补空格。
     *
     * 不能用 `str_pad()` / `%-16s`：它们按字节数算，一个汉字占 3 字节却只显示 2 列，
     * 于是「队列」这种中文表头会把整张表挤歪。`mb_strwidth()` 算的才是列数。
     */
    private static function pad(string $value, int $width): string
    {
        $gap = $width - mb_strwidth($value);

        return $gap > 0 ? $value . str_repeat(' ', $gap) : $value;
    }

    private function line(string $message): void
    {
        fwrite($this->stream, $message . \PHP_EOL);
    }

    private function error(string $message): void
    {
        fwrite($this->stream, "\033[31m错误：" . $message . "\033[0m" . \PHP_EOL);
    }
}
