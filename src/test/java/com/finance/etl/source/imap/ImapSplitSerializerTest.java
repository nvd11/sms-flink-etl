package com.finance.etl.source.imap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 ImapSplitSerializer 版本化编解码器的单元测试
 */
public class ImapSplitSerializerTest {

    private final ImapSplitSerializer serializer = new ImapSplitSerializer();

    @Test
    @DisplayName("测试序列化与反序列化：携带具体 UID 列表的工单")
    public void testSerializeAndDeserializeWithSpecificUids() throws IOException {
        ImapSplit original = new ImapSplit("split-alice-01", "INBOX", Arrays.asList(1082L, 1085L, 1088L));

        byte[] serialized = serializer.serialize(original);
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        ImapSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);
        assertEquals(original, deserialized);
        assertEquals("split-alice-01", deserialized.splitId());
        assertEquals("INBOX", deserialized.getFolderName());
        assertEquals(3, deserialized.getSpecificUids().size());
        assertEquals(1085L, deserialized.getSpecificUids().get(1));
    }

    @Test
    @DisplayName("测试序列化与反序列化：携带 UID 范围的工单")
    public void testSerializeAndDeserializeWithRange() throws IOException {
        ImapSplit original = new ImapSplit("split-range-99", "Finance", 5000L, 8000L, Collections.emptyList());

        byte[] serialized = serializer.serialize(original);
        ImapSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(original, deserialized);
        assertEquals(5000L, deserialized.getStartUid());
        assertEquals(8000L, deserialized.getEndUid());
        assertTrue(deserialized.getSpecificUids().isEmpty());
    }

    @Test
    @DisplayName("测试反序列化不支持的版本时应抛出 IOException")
    public void testUnsupportedVersionThrowsException() throws IOException {
        ImapSplit split = new ImapSplit("split-test", "INBOX", Collections.singletonList(1L));
        byte[] bytes = serializer.serialize(split);

        assertThrows(IOException.class, () -> {
            serializer.deserialize(999, bytes);
        }, "不兼容的版本号应抛出 IOException");
    }
}
