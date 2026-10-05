package com.github.lechandonga.webhookdeliveryservicejava.service;

import org.springframework.http.HttpStatus;

/**
 * 业务异常，携带稳定的机器可读错误码，便于调用方区分失败原因。
 */
public class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final String code;

    public ApiException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }
}
