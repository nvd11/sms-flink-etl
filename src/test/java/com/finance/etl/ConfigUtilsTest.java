package com.finance.etl;

import com.finance.etl.util.ConfigUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证 ConfigUtils 自动加载 .env 环境变量的能力
 */
public class ConfigUtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtilsTest.class);

    @Test
    @DisplayName("测试 ConfigUtils 自动加载本地 .env 敏感配置项")
    public void testDotenvLoading() {
        LOG.info("🧪 [ConfigUtilsTest] Testing .env configuration loading...");

        // 从 .env 获取 Gmail 配置
        String gmailUser = ConfigUtils.get("GMAIL_IMAP_USER");
        String gmailHost = ConfigUtils.get("GMAIL_IMAP_HOST", "imap.gmail.com");
        int gmailPort = ConfigUtils.getInt("GMAIL_IMAP_PORT", 993);

        LOG.info("  ✓ GMAIL_IMAP_USER: {}", gmailUser);
        LOG.info("  ✓ GMAIL_IMAP_HOST: {}", gmailHost);
        LOG.info("  ✓ GMAIL_IMAP_PORT: {}", gmailPort);

        assertNotNull(gmailUser, "GMAIL_IMAP_USER 必须从 .env 成功加载");
        assertEquals("alice.h.y.he@gmail.com", gmailUser, "邮箱账号必须匹配");
        assertEquals("imap.gmail.com", gmailHost, "IMAP 主机必须匹配");
        assertEquals(993, gmailPort, "IMAP 端口必须为 993");

        // 从 .env 获取 Cloudflare R2 存储桶配置
        String r2Bucket = ConfigUtils.get("R2_BUCKET_NAME");
        String r2Endpoint = ConfigUtils.get("R2_S3_ENDPOINT");

        LOG.info("  ✓ R2_BUCKET_NAME: {}", r2Bucket);
        LOG.info("  ✓ R2_S3_ENDPOINT: {}", r2Endpoint);

        assertNotNull(r2Bucket, "R2_BUCKET_NAME 必须从 .env 成功加载");
        assertTrue(r2Bucket.startsWith("sms-flink-etl"), "R2 存储桶名称必须以 sms-flink-etl 开头 (支持 sms-flink-etl 与 sms-flink-etl-dev)");
        assertTrue(r2Endpoint.contains("r2.cloudflarestorage.com"), "R2 Endpoint 域名必须匹配");

        // 默认保底值测试
        String nonExist = ConfigUtils.get("NON_EXISTENT_KEY", "fallback_default_value");
        assertEquals("fallback_default_value", nonExist, "不存在的 Key 应返回提供的保底值");

        LOG.info("🎉 [ConfigUtilsTest] All .env loading assertions passed cleanly!");
    }
}
