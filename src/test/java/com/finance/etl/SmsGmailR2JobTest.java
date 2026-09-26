package com.finance.etl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门针对 SmsGmailR2Job 主作业的 HelloWorld 冒烟与执行验证单测
 */
public class SmsGmailR2JobTest {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2JobTest.class);

    @Test
    @DisplayName("测试 SmsGmailR2Job.main() 本地端到端执行：验证 Flink 能够无异常启动、跑完并正常关闭")
    public void testSmsGmailR2JobMainExecution() {
        LOG.info("================================================================================");
        LOG.info("🧪 [SmsGmailR2JobTest] Starting Local Smoke Test for SmsGmailR2Job...");
        LOG.info("================================================================================");

        // 重定向 System.out 捕获 .print() 的输出内容
        PrintStream originalOut = System.out;
        ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut));

        String consoleOutput;
        try {
            LOG.info("▶️ Invoking SmsGmailR2Job.main(String[] args)...");

            // 执行 SmsGmailR2Job 主入口
            assertDoesNotThrow(() -> {
                SmsGmailR2Job.main(new String[]{});
            }, "SmsGmailR2Job.main() 在本地执行时不应抛出任何异常");

            consoleOutput = capturedOut.toString();

        } finally {
            // 无论测试成功与否，立即恢复标准 System.out，保证后续 Logger 输出正常打印到终端
            System.setOut(originalOut);
        }

        LOG.info("✅ SmsGmailR2Job.main() executed cleanly without exceptions.");
        LOG.info("🔍 Captured console output length: {} characters", consoleOutput.length());

        // 验证核心标识与业务内容是否都被 Flink 算子成功处理并打印
        assertNotNull(consoleOutput, "控制台输出不应为空");

        assertTrue(consoleOutput.contains("[sms-gmail-r2]"), "输出中必须包含专属业务标识 [sms-gmail-r2]");
        LOG.info("  ✓ Verified presence of business tag '[sms-gmail-r2]'");

        assertTrue(consoleOutput.contains("Hello Boss Jason!"), "输出中必须包含 Hello Boss 问候信息");
        LOG.info("  ✓ Verified greeting message 'Hello Boss Jason!'");

        assertTrue(consoleOutput.contains("iceberg.finance.raw_sms_records"), "输出中必须包含目标湖仓表名称");
        LOG.info("  ✓ Verified target Iceberg table 'iceberg.finance.raw_sms_records'");

        assertTrue(consoleOutput.contains("alice.h.y.he@gmail.com"), "输出中必须包含源邮箱账号");
        LOG.info("  ✓ Verified source Gmail account 'alice.h.y.he@gmail.com'");

        LOG.info("================================================================================");
        LOG.info("🎉 [SmsGmailR2JobTest] All Assertions Passed Successfully!");
        LOG.info("================================================================================");
    }
}
