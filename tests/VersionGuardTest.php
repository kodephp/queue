<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use Kode\Queue\Console\Application;
use PHPUnit\Framework\TestCase;

/**
 * 版本常量防漂移：`Application::VERSION` 打在 `queue` / `queue --help` 横幅上，
 * 落后 composer.json 就是对外报错版本（v2.2.0 常量曾一路跟到 2.3.0 发布）。
 */
final class VersionGuardTest extends TestCase
{
    public function testVersionConstantMatchesComposerManifest(): void
    {
        $manifest = json_decode(
            (string) file_get_contents(__DIR__ . '/../composer.json'),
            true,
            512,
            JSON_THROW_ON_ERROR
        );

        self::assertSame(
            $manifest['version'],
            Application::VERSION,
            'src/Console/Application.php 的 VERSION 与 composer.json 的 version 不一致，发版时漏改了一处'
        );
    }
}
