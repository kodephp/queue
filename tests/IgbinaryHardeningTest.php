<?php

declare(strict_types=1);

namespace Kode\Queue\Tests;

use Kode\Queue\Exception\SerializationException;
use Kode\Queue\Message\Job;
use Kode\Queue\Serializer\IgbinarySerializer;
use PHPUnit\Framework\TestCase;

/**
 * IgbinarySerializer 反序列化对象注入硬化
 *
 * 覆盖：
 *  - optionsSupported()：igbinary_unserialize 第二参能力静态探测（不依赖扩展存在）
 *  - containsIncomplete()：__PHP_Incomplete_Class 递归探测（白名单模式残留拒绝）
 *  - 扩展存在时：默认白名单拒绝类负载、allowedClasses=true 恢复旧语义（跳过组）
 */
final class IgbinaryHardeningTest extends TestCase
{
    private function invokePrivate(string $method, mixed ...$args): mixed
    {
        $ref = new \ReflectionMethod(IgbinarySerializer::class, $method);

        return $ref->invoke(null, ...$args);
    }

    public function testOptionsSupportedMatchesFunctionExistence(): void
    {
        $supported = $this->invokePrivate('optionsSupported');
        $this->assertIsBool($supported);

        if (\extension_loaded('igbinary')) {
            // 有扩展时结论必须与真实参数表一致
            $params = (new \ReflectionFunction('igbinary_unserialize'))->getNumberOfParameters();
            $this->assertSame($params >= 2, $supported);
        } else {
            $this->assertFalse($supported, '无 igbinary 函数时不得报告支持选项');
        }
    }

    public function testContainsIncompleteDetectsNestedObjects(): void
    {
        // 反序列化一个不存在的类 → 天然得到 __PHP_Incomplete_Class
        $incomplete = @unserialize('O:7:"NoExist":1:{s:1:"a";i:1;}');
        $this->assertInstanceOf(\__PHP_Incomplete_Class::class, $incomplete);

        $this->assertTrue($this->invokePrivate('containsIncomplete', $incomplete));
        $this->assertTrue($this->invokePrivate('containsIncomplete', ['a' => [1, ['b' => $incomplete]]]));
        $this->assertFalse($this->invokePrivate('containsIncomplete', ['a' => [1, 'x', null, ['k' => 'v']]]));
        $this->assertFalse($this->invokePrivate('containsIncomplete', 'scalar'));
        $this->assertFalse($this->invokePrivate('containsIncomplete', null));
    }

    public function testConstructorRejectsWithoutExtension(): void
    {
        if (\extension_loaded('igbinary')) {
            $this->markTestSkipped('igbinary 已安装：构造器不应抛错');
        }

        $this->expectException(SerializationException::class);
        new IgbinarySerializer();
    }

    public function testDefaultRejectsClassBearingPayload(): void
    {
        if (! \extension_loaded('igbinary')) {
            $this->markTestSkipped('缺少 ext-igbinary');
        }

        $job = Job::create('demo', ['k' => 'v']);
        $raw = (new IgbinarySerializer())->serialize($job);

        // 默认白名单：本负载纯数据，往返必须成功
        $restored = (new IgbinarySerializer())->deserialize($raw);
        $this->assertSame($job->name, $restored->name);
        $this->assertSame(['k' => 'v'], $restored->payload);
    }

    public function testTrueFlagRestoresObjectRevivalForTrustedPayloads(): void
    {
        if (! \extension_loaded('igbinary')) {
            $this->markTestSkipped('缺少 ext-igbinary');
        }

        $job = Job::create('demo', ['obj' => (object) ['a' => 1]]);
        $raw = (new IgbinarySerializer(true))->serialize($job);
        $restored = (new IgbinarySerializer(true))->deserialize($raw);

        $this->assertInstanceOf(\stdClass::class, $restored->payload['obj']);

        // 默认模式下同一负载应被拒绝（夹带类 → Incomplete → decodeFailed）
        $this->expectException(SerializationException::class);
        (new IgbinarySerializer())->deserialize($raw);
    }
}
