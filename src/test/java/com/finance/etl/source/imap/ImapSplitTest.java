package com.finance.etl.source.imap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 ImapSplit 数据工单 POJO 的单元测试
 */
public class ImapSplitTest {

    @Test
    @DisplayName("测试 ImapSplit 基础构造与字段完整性")
    public void testImapSplitWithSpecificUids() {
        List<Long> uids = Arrays.asList(1001L, 1002L, 1003L);
        ImapSplit split = new ImapSplit("split-inbox-0", "INBOX", uids);

        assertEquals("split-inbox-0", split.splitId());
        assertEquals("INBOX", split.getFolderName());
        assertEquals(3, split.getSpecificUids().size());
        assertEquals(1001L, split.getSpecificUids().get(0));
    }

    @Test
    @DisplayName("测试 ImapSplit 基于 UID 区间范围的构造")
    public void testImapSplitWithRange() {
        ImapSplit split = new ImapSplit("split-range-1", "INBOX", 100L, 200L);

        assertEquals("split-range-1", split.splitId());
        assertEquals("INBOX", split.getFolderName());
        assertEquals(100L, split.getStartUid());
        assertEquals(200L, split.getEndUid());
        assertTrue(split.getSpecificUids().isEmpty());
    }

    @Test
    @DisplayName("测试 ImapSplit 的 equals 与 hashCode 契约")
    public void testImapSplitEquality() {
        ImapSplit split1 = new ImapSplit("split-1", "INBOX", Arrays.asList(1L, 2L));
        ImapSplit split2 = new ImapSplit("split-1", "INBOX", Arrays.asList(1L, 2L));
        ImapSplit split3 = new ImapSplit("split-2", "INBOX", Arrays.asList(1L, 2L));

        assertEquals(split1, split2);
        assertEquals(split1.hashCode(), split2.hashCode());
        assertNotEquals(split1, split3);
    }
}
