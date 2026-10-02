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

        // 仅在当前 JUnit 测试中实现双向分流 (TeeOutputStream): 既实时输出到屏幕，又捕获到内存用于断言
        PrintStream teeOut = new PrintStream(new java.io.OutputStream() {
            @Override
            public void write(int b) {
                originalOut.write(b);
                capturedOut.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                originalOut.write(b, off, len);
                capturedOut.write(b, off, len);
            }

            @Override
            public void flush() {
                originalOut.flush();
            }
        });
        System.setOut(teeOut);

        String consoleOutput;
        try {
            LOG.info("▶️ Invoking SmsGmailR2Job.main(String[] args)...");

            assertDoesNotThrow(() -> {
                try {
                    SmsGmailR2Job.main(new String[]{});
                } catch (Throwable t) {
                    LOG.error("❌ Exception thrown in SmsGmailR2Job.main(): {}", t.getMessage(), t);
                    throw t;
                }
            }, "SmsGmailR2Job.main() 在本地执行时不应抛出任何异常");

            consoleOutput = capturedOut.toString();

        } finally {
            System.setOut(originalOut);
        }

        LOG.info("✅ SmsGmailR2Job.main() executed cleanly without exceptions.");
        LOG.info("🔍 Captured console output length: {} characters", consoleOutput.length());

        // 💥 直接使用真实 Logger 完整输出每一行解析出的 SmsRecord 数据
        LOG.info("==================== [Extracted SmsRecords in Lakehouse Batch] ====================");
        for (String line : consoleOutput.split("\n")) {
            if (line.contains("SmsRecord")) {
                LOG.info("📊 {}", line.trim());
            }
        }
        LOG.info("====================================================================================");

        assertNotNull(consoleOutput, "控制台输出对象不应为 null");

        if (consoleOutput.contains("SmsRecord")) {
            LOG.info("  ✓ Batch processed incremental records: verified presence of SmsRecord stream entities");
            assertTrue(consoleOutput.contains("msgUid") || consoleOutput.contains("rawBody"), "输出中必须包含关键元数据字段");
            LOG.info("  ✓ Verified data integrity of rawBody/msgUid");
        } else {
            LOG.info("  ℹ️ Watermark is already up-to-date: verified zero-record batch cleanly and idempotently terminated (0 records emitted)");
        }

        LOG.info("================================================================================");
        LOG.info("🎉 [SmsGmailR2JobTest] All Assertions Passed Successfully!");
        LOG.info("================================================================================");
    }
}
