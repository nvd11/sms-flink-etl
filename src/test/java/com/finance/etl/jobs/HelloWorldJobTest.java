package com.finance.etl.jobs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * 专门针对 HelloWorldJob 的单元冒烟测试
 */
public class HelloWorldJobTest {

    @Test
    @DisplayName("测试 HelloWorldJob.main()：验证 Flink 批处理本地 MiniCluster 启动与执行无异常")
    public void testHelloWorldJobExecution() {
        assertDoesNotThrow(() -> {
            HelloWorldJob.main(new String[]{});
        }, "HelloWorldJob.main() 在本地执行时不应抛出任何异常");
    }
}
