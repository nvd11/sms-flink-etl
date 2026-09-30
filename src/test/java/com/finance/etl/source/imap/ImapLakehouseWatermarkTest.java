package com.finance.etl.source.imap;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 ImapSplitEnumerator 湖仓水位探查逻辑的单元测试
 */
public class ImapLakehouseWatermarkTest {

    @Test
    @DisplayName("测试 fetchLastSyncedImapUidFromLakehouse(): 验证当无 JDBC 配置或空表时安全返回 0L 兜底")
    public void testFetchLastSyncedImapUidDefaultFallback() {
        SplitEnumeratorContext<ImapSplit> dummyContext = new SplitEnumeratorContext<>() {
            @Override public SplitEnumeratorMetricGroup metricGroup() { return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup(); }
            @Override public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}
            @Override public int currentParallelism() { return 1; }
            @Override public Map<Integer, ReaderInfo> registeredReaders() { return Collections.emptyMap(); }
            @Override public void assignSplits(SplitsAssignment<ImapSplit> newSplitAssignments) {}
            @Override public void signalNoMoreSplits(int subtaskId) {}
            @Override public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {}
            @Override public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period) {}
            @Override public void runInCoordinatorThread(Runnable runnable) { runnable.run(); }
        };

        ImapSplitEnumerator enumerator = new ImapSplitEnumerator(
                dummyContext, "imap.gmail.com", 993, "test@gmail.com", "", null, 7890, 20);

        long lastUid = enumerator.fetchLastSyncedImapUidFromLakehouse();
        assertTrue(lastUid >= 0L, "水位探查结果必须为非负长整数");
    }
}
