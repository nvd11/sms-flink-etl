package com.finance.etl.sink.iceberg;

import com.finance.etl.model.SyncOffset;
import com.finance.etl.util.ConfigUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("IcebergOffsetRepository 湖仓水位元数据仓储服务测试")
class IcebergOffsetRepositoryTest {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergOffsetRepositoryTest.class);

    @Test
    @DisplayName("测试从环境配置装配 IcebergOffsetRepository 实体")
    void testFromConfig() {
        IcebergOffsetRepository repo = IcebergOffsetRepository.fromConfig();
        assertNotNull(repo);
        assertNotNull(repo.getSchemaName());
        assertEquals("etl_sync_offsets", repo.getTableName());
    }

    @Test
    @DisplayName("测试真实湖仓 etl_sync_offsets 水位读取能力")
    void testGetLatestOffset() throws Exception {
        String uri = ConfigUtils.get("ICEBERG_CATALOG_URI");
        if (uri == null || uri.trim().isEmpty() || uri.contains("<host>")) {
            LOG.warn("⚠️ [Test] No real Iceberg catalog configured. Skipping live network test.");
            return;
        }

        try (IcebergOffsetRepository repo = IcebergOffsetRepository.fromConfig()) {
            long latestOffset = repo.getLatestOffset("test-offset-job", "EMAIL_IMAP", "alice.h.y.he@gmail.com");
            LOG.info("🧪 [Test] Discovered latest offset: {}", latestOffset);
            assertTrue(latestOffset >= 0L, "读取到的水位应当大于等于 0");
        }
    }

    @Test
    @DisplayName("测试真实湖仓水位保存与回读闭环验证 (使用独立测试 job_name，避免污染主作业水位)")
    void testSaveAndReadOffsetRoundtrip() throws Exception {
        String uri = ConfigUtils.get("ICEBERG_CATALOG_URI");
        if (uri == null || uri.trim().isEmpty() || uri.contains("<host>")) {
            LOG.warn("⚠️ [Test] No real Iceberg catalog configured. Skipping live network test.");
            return;
        }

        try (IcebergOffsetRepository repo = IcebergOffsetRepository.fromConfig()) {
            long currentOffset = repo.getLatestOffset("test-offset-job", "EMAIL_IMAP", "alice.h.y.he@gmail.com");

            long nextOffset = Math.max(currentOffset + 1, 100L);
            SyncOffset offset = new SyncOffset(
                    "test-offset-job",
                    "EMAIL_IMAP",
                    "alice.h.y.he@gmail.com",
                    nextOffset,
                    Instant.now(),
                    Instant.now()
            );

            repo.saveOffset(offset);

            long updatedOffset = repo.getLatestOffset("test-offset-job", "EMAIL_IMAP", "alice.h.y.he@gmail.com");
            LOG.info("🧪 [Test] Offset after commit: {} (expected >= {})", updatedOffset, nextOffset);
            assertTrue(updatedOffset >= nextOffset, "提交新位点后读取的水位应当成功推进");
        }
    }
}
