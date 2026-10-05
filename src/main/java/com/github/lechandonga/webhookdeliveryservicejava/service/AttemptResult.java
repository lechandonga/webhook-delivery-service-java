package com.github.lechandonga.webhookdeliveryservicejava.service;

/**
 * 单次 HTTP 投递尝试的结果分类。
 *
 * @param success   订阅方是否返回 2xx
 * @param retryable 是否属于可重试失败（超时、连不上、5xx）
 * @param detail    用于排查的原因描述
 */
public record AttemptResult(boolean success, boolean retryable, String detail) {

    public static AttemptResult ok(int statusCode) {
        return new AttemptResult(true, false, "HTTP " + statusCode);
    }

    public static AttemptResult businessFailure(int statusCode) {
        return new AttemptResult(false, false,
                "HTTP " + statusCode + " 业务性失败（4xx），不重试，直接进入死信");
    }

    public static AttemptResult retryableFailure(int statusCode) {
        return new AttemptResult(false, true, "HTTP " + statusCode + " 服务端错误（5xx）");
    }

    public static AttemptResult transportFailure(String detail) {
        return new AttemptResult(false, true, detail);
    }
}
