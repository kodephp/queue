<?php

namespace Kode\Queue\Util;

/**
 * 队列工具类
 * 
 * 提供队列操作中常用的工具方法，包括任务ID生成、负载创建与解析、
 * 名称验证、延迟计算、指数退避等功能。
 */
class QueueUtil {
    /**
     * 生成唯一的任务ID
     *
     * @return string 唯一的任务ID
     */
    public static function generateJobId(): string {
        return uniqid('', true);
    }

    /**
     * 创建任务负载JSON字符串
     *
     * @param mixed $job 任务名称或闭包
     * @param array $data 任务数据
     * @return string JSON格式的任务负载
     */
    public static function createPayload($job, array $data = []): string {
        return json_encode([
            'job' => $job,
            'data' => $data,
            'id' => self::generateJobId(),
            'attempts' => 0,
            'created_at' => time(),
        ]);
    }

    /**
     * 解析任务负载JSON字符串
     *
     * @param string $payload JSON格式的任务负载
     * @return array 解析后的任务数据数组
     */
    public static function parsePayload(string $payload): array {
        $data = json_decode($payload, true);
        return is_array($data) ? $data : [];
    }

    /**
     * 检查任务是否已就绪（可用于延迟任务）
     *
     * @param array $job 任务数据
     * @return bool 是否已就绪
     */
    public static function isJobReady(array $job): bool {
        if (!isset($job['available_at'])) {
            return true;
        }
        return $job['available_at'] <= time();
    }

    /**
     * 计算延迟后的时间戳
     *
     * @param int $delay 延迟秒数
     * @return int 延迟后的时间戳
     */
    public static function calculateDelay(int $delay): int {
        return time() + $delay;
    }

    /**
     * 格式化队列大小为人类可读格式
     *
     * @param int $size 队列大小
     * @return string 格式化后的大小字符串（如 1.50K, 2.30M）
     */
    public static function formatSize(int $size): string {
        if ($size >= 1000000) {
            return number_format($size / 1000000, 2) . 'M';
        }
        if ($size >= 1000) {
            return number_format($size / 1000, 2) . 'K';
        }
        return (string)$size;
    }

    /**
     * 验证队列名称是否合法
     * 
     * 合法的队列名称只能包含字母、数字、下划线、连字符和点
     *
     * @param string $queue 队列名称
     * @return bool 是否合法
     */
    public static function validateQueueName(string $queue): bool {
        return preg_match('/^[a-zA-Z0-9_\-\.]+$/', $queue) === 1;
    }

    /**
     * 验证任务名称是否合法
     * 
     * 合法的任务名称必须以字母或下划线开头，只能包含字母、数字、下划线和反斜杠
     *
     * @param string $job 任务名称
     * @return bool 是否合法
     */
    public static function validateJobName(string $job): bool {
        return preg_match('/^[a-zA-Z_][a-zA-Z0-9_\\\\]*$/', $job) === 1;
    }

    /**
     * 计算指数退避延迟时间
     * 
     * 用于任务重试时计算递增的延迟时间，避免立即重试导致系统压力过大
     *
     * @param int $attempt 当前重试次数（从1开始）
     * @param int $baseDelay 基础延迟时间（毫秒）
     * @param float $multiplier 延迟乘数
     * @return int 计算后的延迟时间（毫秒）
     * 
     * @example
     * // 第1次重试：100ms
     * // 第2次重试：200ms
     * // 第3次重试：400ms
     * QueueUtil::exponentialBackoff(1, 100, 2.0); // 100
     * QueueUtil::exponentialBackoff(2, 100, 2.0); // 200
     * QueueUtil::exponentialBackoff(3, 100, 2.0); // 400
     */
    public static function exponentialBackoff(int $attempt, int $baseDelay = 100, float $multiplier = 2.0): int {
        return (int)($baseDelay * pow($multiplier, $attempt - 1));
    }

    /**
     * 检查值是否可序列化
     *
     * @param mixed $value 要检查的值
     * @return bool 是否可序列化
     */
    public static function isSerializable($value): bool {
        try {
            serialize($value);
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }
}
