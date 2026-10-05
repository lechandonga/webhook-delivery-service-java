package com.github.lechandonga.webhookdeliveryservicejava.service;

/** 待投递事件积压超过上限时抛出。 */
public class BackpressureException extends RuntimeException {

    public BackpressureException(int pending, int maxPending) {
        super("待投递事件积压超限，拒绝新的发布: pending=" + pending + ", maxPending=" + maxPending);
    }
}
