package com.github.lechandonga.webhookdeliveryservicejava.tests;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在测试日志中显式打印每个业务场景的开始与执行结果，
 * 便于从测试输出直接看出覆盖了哪些场景。
 */
public class ScenarioLoggingExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

    private static final Logger log = LoggerFactory.getLogger("BUSINESS-SCENARIO");

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        log.info("================ 场景开始: {} ================", context.getDisplayName());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        boolean failed = context.getExecutionException().isPresent();
        if (failed) {
            log.error("================ 场景结果: {} => 失败 ================", context.getDisplayName());
        } else {
            log.info("================ 场景结果: {} => 通过 ================", context.getDisplayName());
        }
    }
}
