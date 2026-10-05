package com.github.lechandonga.webhookdeliveryservicejava.service;

/** 同一事件标识被以不同内容重复提交时抛出。 */
public class DuplicateEventException extends RuntimeException {

    public DuplicateEventException(String eventId) {
        super("事件已存在但内容不一致，拒绝重复发布: eventId=" + eventId);
    }
}
