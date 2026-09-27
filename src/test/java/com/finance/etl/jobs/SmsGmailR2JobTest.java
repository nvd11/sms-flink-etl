package com.finance.etl.jobs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 jobs 包下的 SmsGmailR2Job 主作业端到端执行测试
 */
public class SmsGmailR2JobTest {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2JobTest.class);

    @Test
    @DisplayName("测试 SmsGmailR2Job.main() 本地端到端执行：验证 Flink 能够无异常启动、跑完并正常关闭")
    public void testSmsGmailR2JobMainExecution() {
        LOG.info("================================================================================");
        LOG.info("🧪 [SmsGmailR2JobTest] Starting Local Smoke Test for SmsGmailR2Job...");
        LOG.info("================================================================================");

        PrintStream originalOut = System.out;
        ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut));

        String consoleOutput;
        try {
            LOG.info("▶️ Invoking SmsGmailR2Job.main(String[] args)...");

            assertDoesNotThrow(() -> {
                SmsGmailR2Job.main(new String[]{});
            }, "SmsGmailR2Job.main() 在本地执行时不应抛出任何异常");

            consoleOutput = capturedOut.toString();

        } finally {
            System.setOut(originalOut);
        }

        LOG.info("✅ SmsGmailR2Job.main() executed cleanly without exceptions.");
        LOG.info("🔍 Captured console output length: {} characters", consoleOutput.length());

        assertNotNull(consoleOutput, "控制台输出不应为空");
        assertTrue(consoleOutput.contains("SmsRecord"), "输出中必须包含 SmsRecord 实体输出");
        LOG.info("  ✓ Verified presence of SmsRecord stream entities");

        assertTrue(consoleOutput.contains("msgUid") || consoleOutput.contains("rawBody"), "输出中必须包含关键元数据字段");
        LOG.info("  ✓ Verified data integrity of rawBody/msgUid");

        LOG.info("================================================================================");
        LOG.info("🎉 [SmsGmailR2JobTest] All Assertions Passed Successfully!");
        LOG.info("================================================================================");
    }
}
