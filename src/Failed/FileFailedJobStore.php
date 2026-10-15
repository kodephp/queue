<?php

declare(strict_types=1);

namespace Kode\Queue\Failed;

use Kode\Queue\Contract\FailedJobStoreInterface;
use Kode\Queue\Exception\QueueException;
use Kode\Queue\Message\Job;
use Kode\Queue\Support\IdGenerator;
use Override;

/**
 * 文件型失败任务存储。
 *
 * 面向「没有数据库、也不想为死信单开一套基础设施」的场景：
 * 单文件小项目、边缘节点、CI 环境、容器里挂了一块卷的 sidecar。
 *
 * 布局按队列分目录，避免单目录堆到几十万文件后 `scandir` 卡死：
 *
 * ```
 * storage/failed/
 *   ├── default-5f2b8c1a/
 *   │     ├── 01J8Z9K3M4N5P6Q7R8S9T0V1W2.json
 *   │     └── 01J8Z9K3M4N5P6Q7R8S9T0V1W3.json
 *   └── mails-9a3d7e04/
 *         └── 01J8Z9K3M4N5P6Q7R8S9T0V1W4.json
 * ```
 *
 * 目录名 = 清洗后的队列名 + 队列名 SHA1 前 8 位，
 * 既保证可读，又保证「order.高优先级」和「order_高优先级」不会撞在一起。
 * 文件名用 ULID，天生按时间有序，所以「按失败时间倒序」只要反向读目录即可，
 * 不需要把所有文件读出来再排序。
 *
 * 写入走「临时文件 + rename」，rename 在同一文件系统内是原子操作，
 * 读取方永远不会看到写了一半的 JSON。
 */
final class FileFailedJobStore implements FailedJobStoreInterface
{
    private const string EXTENSION = '.json';

    public function __construct(
        private readonly string $directory,
        private readonly int $directoryMode = 0o775,
    ) {
        if ($directory === '') {
            throw new QueueException('FileFailedJobStore 需要一个有效的存储目录');
        }
    }

    #[Override]
    public function store(Job $job, \Throwable $exception, string $connection = 'default'): string
    {
        $id = IdGenerator::ulid();
        $bucket = $this->bucket($job->queue);

        $this->ensureDirectory($bucket);

        $record = [
            'id' => $id,
            'connection' => $connection,
            'queue' => $job->queue,
            'job' => $job->toArray(),
            'exception' => ArrayFailedJobStore::format($exception),
            'failed_at' => time(),
        ];

        $this->atomicWrite(
            $bucket . \DIRECTORY_SEPARATOR . $id . self::EXTENSION,
            json_encode($record, \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES | \JSON_PRETTY_PRINT)
                ?: throw new QueueException('死信序列化失败：' . json_last_error_msg()),
        );

        return $id;
    }

    #[Override]
    public function find(string $id): ?array
    {
        $path = $this->locate($id);

        return $path === null ? null : $this->read($path);
    }

    #[Override]
    public function all(?string $queue = null, int $limit = 100, int $offset = 0): array
    {
        if ($limit <= 0) {
            return [];
        }

        $records = [];
        $skipped = 0;

        // ULID 文件名天然有序：倒序遍历即「最新的排在前面」，不用全量读盘再排序
        foreach ($this->files($queue) as $path) {
            if ($skipped < $offset) {
                $skipped++;

                continue;
            }

            $record = $this->read($path);

            if ($record !== null) {
                $records[] = $record;
            }

            if (\count($records) >= $limit) {
                break;
            }
        }

        return $records;
    }

    #[Override]
    public function count(?string $queue = null): int
    {
        return iterator_count($this->files($queue));
    }

    #[Override]
    public function forget(string $id): bool
    {
        $path = $this->locate($id);

        return $path !== null && @unlink($path);
    }

    #[Override]
    public function flush(?string $queue = null, ?int $olderThan = null): int
    {
        $removed = 0;

        foreach ($this->files($queue) as $path) {
            if ($olderThan !== null) {
                $modified = @filemtime($path);

                if ($modified !== false && $modified > $olderThan) {
                    continue;
                }
            }

            if (@unlink($path)) {
                $removed++;
            }
        }

        $this->pruneEmptyBuckets();

        return $removed;
    }

    /**
     * 存储根目录（运维排查时直接 `ls` 用）。
     */
    public function directory(): string
    {
        return $this->directory;
    }

    /**
     * 占用的磁盘空间（字节），用于容量告警。
     */
    public function diskUsage(?string $queue = null): int
    {
        $bytes = 0;

        foreach ($this->files($queue) as $path) {
            $size = @filesize($path);
            $bytes += $size === false ? 0 : $size;
        }

        return $bytes;
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /**
     * 倒序（最新优先）产出所有死信文件路径。
     *
     * @return \Generator<int, string>
     */
    private function files(?string $queue): \Generator
    {
        foreach ($this->buckets($queue) as $bucket) {
            $entries = @scandir($bucket, \SCANDIR_SORT_DESCENDING);

            if ($entries === false) {
                continue;
            }

            foreach ($entries as $entry) {
                if (!str_ends_with($entry, self::EXTENSION)) {
                    continue;
                }

                yield $bucket . \DIRECTORY_SEPARATOR . $entry;
            }
        }
    }

    /**
     * @return list<string>
     */
    private function buckets(?string $queue): array
    {
        if ($queue !== null) {
            $bucket = $this->bucket($queue);

            return is_dir($bucket) ? [$bucket] : [];
        }

        if (!is_dir($this->directory)) {
            return [];
        }

        $found = glob($this->directory . \DIRECTORY_SEPARATOR . '*', \GLOB_ONLYDIR);

        return $found === false ? [] : $found;
    }

    private function bucket(string $queue): string
    {
        $safe = preg_replace('/[^A-Za-z0-9_.-]+/u', '_', $queue) ?? 'queue';
        $safe = trim(mb_substr($safe, 0, 48), '._-');

        return $this->directory
            . \DIRECTORY_SEPARATOR
            . ($safe === '' ? 'queue' : $safe) . '-' . substr(sha1($queue), 0, 8);
    }

    private function locate(string $id): ?string
    {
        if (!$this->isValidId($id)) {
            return null;
        }

        $matches = glob(
            $this->directory . \DIRECTORY_SEPARATOR . '*' . \DIRECTORY_SEPARATOR . $id . self::EXTENSION,
        );

        return $matches === false || $matches === [] ? null : $matches[0];
    }

    /**
     * 只允许 ULID / 十六进制形态的 ID 参与路径拼接，杜绝 `../` 穿越。
     */
    private function isValidId(string $id): bool
    {
        return $id !== '' && preg_match('/^[0-9A-Za-z]{8,64}$/', $id) === 1;
    }

    /**
     * @return array{id: string, connection: string, queue: string, job: Job, exception: string, failed_at: int}|null
     */
    private function read(string $path): ?array
    {
        $raw = @file_get_contents($path);

        if ($raw === false || !json_validate($raw)) {
            return null;
        }

        /** @var array<string, mixed> $data */
        $data = json_decode($raw, true, 512, \JSON_THROW_ON_ERROR);

        return [
            'id' => (string) ($data['id'] ?? basename($path, self::EXTENSION)),
            'connection' => (string) ($data['connection'] ?? 'default'),
            'queue' => (string) ($data['queue'] ?? 'default'),
            'job' => Job::fromArray((array) ($data['job'] ?? [])),
            'exception' => (string) ($data['exception'] ?? ''),
            'failed_at' => (int) ($data['failed_at'] ?? 0),
        ];
    }

    private function atomicWrite(string $path, string $contents): void
    {
        $temp = $path . '.' . getmypid() . '.tmp';

        if (@file_put_contents($temp, $contents, \LOCK_EX) === false) {
            throw new QueueException(sprintf('无法写入死信文件：%s', $path));
        }

        if (!@rename($temp, $path)) {
            @unlink($temp);

            throw new QueueException(sprintf('无法提交死信文件：%s', $path));
        }
    }

    private function ensureDirectory(string $path): void
    {
        if (is_dir($path)) {
            return;
        }

        // 并发下可能被别的进程抢先建好，所以建完再确认一次而不是直接信 mkdir 的返回值
        if (!@mkdir($path, $this->directoryMode, true) && !is_dir($path)) {
            throw new QueueException(sprintf('无法创建死信目录：%s', $path));
        }
    }

    private function pruneEmptyBuckets(): void
    {
        foreach ($this->buckets(null) as $bucket) {
            $entries = @scandir($bucket);

            if ($entries !== false && \count($entries) <= 2) {
                @rmdir($bucket);
            }
        }
    }
}
