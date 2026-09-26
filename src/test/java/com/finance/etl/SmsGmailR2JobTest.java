package com.finance.etl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门针对 SmsGmailR2Job 主作业的 HelloWorld 冒烟与执行验证单测
 */
public class SmsGmailR2JobTest {

    @Test
    @DisplayName("测试 SmsGmailR2Job.main() 本地端到端执行：验证 Flink 能够无异常启动、跑完并正常关闭")
    public void testSmsGmailR2JobMainExecution() {
        // 重定向 System.out 捕获 .print() 的输出内容
        PrintStream originalOut = System.out;
        ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut));

        try {
            // 执行 SmsGmailR2Job 主入口
            assertDoesNotThrow(() -> {
                SmsGmailR2Job.main(new String[]{});
            }, "SmsGmailR2Job.main() 在本地执行时不应抛出任何异常");

            // 检查输出流内容
            String consoleOutput = capturedOut.toString();
            assertNotNull(consoleOutput, "控制台输出不应为空");

            // 验证核心标识与业务内容是否都被 Flink 算子成功处理并打印
            assertTrue(consoleOutput.contains("[sms-gmail-r2]"), "输出中必须包含专属业务标识 [sms-gmail-r2]");
            assertTrue(consoleOutput.contains("Hello Boss Jason!"), "输出中必须包含 Hello Boss 问候信息");
            assertTrue(consoleOutput.contains("iceberg.finance.raw_sms_records"), "输出中必须包含目标湖仓表名称");
            assertTrue(consoleOutput.contains("alice.h.y.he@gmail.com"), "输出中必须包含源邮箱账号");

        } finally {
            // 无论测试成功与否，恢复标准 System.out
            System.setOut(originalOut);
        }
    }
}
