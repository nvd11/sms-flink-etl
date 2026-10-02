package com.finance.etl.source.imap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ImapSyncMode 邮件消费模式枚举与解析测试")
class ImapSyncModeTest {

    @Test
    @DisplayName("测试 fromString 解析能力与安全兜底")
    void testFromString() {
        assertEquals(ImapSyncMode.EARLIEST_FIRST, ImapSyncMode.fromString("EARLIEST_FIRST"));
        assertEquals(ImapSyncMode.EARLIEST_FIRST, ImapSyncMode.fromString("earliest_first"));
        assertEquals(ImapSyncMode.EARLIEST_FIRST, ImapSyncMode.fromString("EARLIEST"));
        assertEquals(ImapSyncMode.EARLIEST_FIRST, ImapSyncMode.fromString(null));
        assertEquals(ImapSyncMode.EARLIEST_FIRST, ImapSyncMode.fromString(""));
        assertEquals(ImapSyncMode.EARLIEST_FIRST, ImapSyncMode.fromString("invalid_mode"));

        assertEquals(ImapSyncMode.LATEST_FIRST, ImapSyncMode.fromString("LATEST_FIRST"));
        assertEquals(ImapSyncMode.LATEST_FIRST, ImapSyncMode.fromString("latest_first"));
        assertEquals(ImapSyncMode.LATEST_FIRST, ImapSyncMode.fromString("LATEST"));
        assertEquals(ImapSyncMode.LATEST_FIRST, ImapSyncMode.fromString("LATEST_WINDOW"));
    }

    @Test
    @DisplayName("测试 ImapSource 构建时正确装配 syncMode")
    void testImapSourceSyncMode() {
        ImapSource sourceEarliest = ImapSource.builder()
                .syncMode(ImapSyncMode.EARLIEST_FIRST)
                .build();
        assertEquals(ImapSyncMode.EARLIEST_FIRST, sourceEarliest.getSyncMode());

        ImapSource sourceLatest = ImapSource.builder()
                .syncMode(ImapSyncMode.LATEST_FIRST)
                .build();
        assertEquals(ImapSyncMode.LATEST_FIRST, sourceLatest.getSyncMode());

        ImapSource fromConfig = ImapSource.fromConfig();
        assertNotNull(fromConfig.getSyncMode());
    }
}
