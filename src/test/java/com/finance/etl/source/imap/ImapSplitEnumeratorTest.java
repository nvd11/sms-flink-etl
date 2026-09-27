package com.finance.etl.source.imap;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 ImapSplitEnumerator 调度决策与派单状态机的单元测试
 */
public class ImapSplitEnumeratorTest {

    @Test
    @DisplayName("测试 Enumerator 启动、工单分发与 signalNoMoreSplits 派单闭环")
    public void testEnumeratorStartAndAssignment() {
        List<ImapSplit> assignedSplits = new ArrayList<>();
        List<Integer> signaledSubtasks = new ArrayList<>();

        SplitEnumeratorContext<ImapSplit> mockContext = new SplitEnumeratorContext<>() {
            @Override
            public SplitEnumeratorMetricGroup metricGroup() {
                return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
            }

            @Override
            public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

            @Override
            public int currentParallelism() {
                return 2;
            }

            @Override
            public Map<Integer, ReaderInfo> registeredReaders() {
                return Collections.emptyMap();
            }

            @Override
            public void assignSplits(SplitsAssignment<ImapSplit> newSplitAssignments) {
                for (List<ImapSplit> list : newSplitAssignments.assignment().values()) {
                    assignedSplits.addAll(list);
                }
            }

            @Override
            public void assignSplit(ImapSplit split, int subtaskId) {
                assignedSplits.add(split);
            }

            @Override
            public void signalNoMoreSplits(int subtaskId) {
                signaledSubtasks.add(subtaskId);
            }

            @Override
            public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {}

            @Override
            public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period) {}

            @Override
            public void runInCoordinatorThread(Runnable runnable) {
                runnable.run();
            }
        };

        ImapSplitEnumerator enumerator = new ImapSplitEnumerator(
                mockContext, "imap.gmail.com", 993, "alice@gmail.com", "", null, 7890, 20);

        // 1. 验证 start() 探查生成至少 1 个工单
        enumerator.start();
        assertTrue(enumerator.getPendingSplitsCount() >= 1, "启动时应探测并生成工单");

        // 2. 模拟 Worker 0 请求工单 -> 触发派单
        enumerator.handleSplitRequest(0, "localhost");
        assertEquals(1, assignedSplits.size(), "Worker 0 应获得一张派发的工单");

        // 3. 模拟 Worker 0 再次请求工单 (已无剩余工单) -> 触发 signalNoMoreSplits
        enumerator.handleSplitRequest(0, "localhost");
        assertTrue(signaledSubtasks.contains(0), "工单分发完毕后应向 Worker 0 发送 signalNoMoreSplits");

        // 4. 模拟 Worker 1 请求工单 -> 直接发送 signalNoMoreSplits
        enumerator.handleSplitRequest(1, "localhost");
        assertTrue(signaledSubtasks.contains(1), "无工单时应向 Worker 1 发送 signalNoMoreSplits");
    }
}
