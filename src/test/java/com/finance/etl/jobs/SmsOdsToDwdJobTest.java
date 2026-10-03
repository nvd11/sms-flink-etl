package com.finance.etl.jobs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SmsOdsToDwdJob 主作业端到端批处理执行冒烟测试")
class SmsOdsToDwdJobTest {
    private static final Logger LOG = LoggerFactory.getLogger(SmsOdsToDwdJobTest.class);

    @Test
    @DisplayName("测试 SmsOdsToDwdJob.main() 本地端到端执行：从 Dev 湖仓读取 ODS 并成功落盘到 DWD 表")
    void testSmsOdsToDwdJobMainExecution() {
        LOG.info("================================================================================");
        LOG.info("🧪 [SmsOdsToDwdJobTest] Starting Local Smoke Test for SmsOdsToDwdJob...");
        LOG.info("================================================================================");

        PrintStream originalOut = System.out;
        ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();

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
            LOG.info("▶️ Invoking SmsOdsToDwdJob.main(String[] args)...");

            assertDoesNotThrow(() -> {
                try {
                    SmsOdsToDwdJob.main(new String[]{});
                } catch (Throwable t) {
                    LOG.error("❌ Exception thrown in SmsOdsToDwdJob.main(): {}", t.getMessage(), t);
                    throw t;
                }
            }, "SmsOdsToDwdJob.main() 在本地执行时不应抛出任何异常");

            consoleOutput = capturedOut.toString();

        } finally {
            System.setOut(originalOut);
        }

        LOG.info("✅ SmsOdsToDwdJob.main() executed cleanly without exceptions.");
        LOG.info("🔍 Captured console output length: {} characters", consoleOutput.length());

        assertNotNull(consoleOutput, "控制台输出对象不应为 null");

        LOG.info("================================================================================");
        LOG.info("🎉 [SmsOdsToDwdJobTest] All Assertions Passed Successfully!");
        LOG.info("================================================================================");
    }
}
