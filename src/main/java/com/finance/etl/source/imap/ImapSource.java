package com.finance.etl.source.imap;

import com.finance.etl.model.RawEmail;
import com.finance.etl.util.ConfigUtils;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.Serializable;

/**
 * Flink FLIP-27 Specification: IMAP Email Data Source Connector Facade (ImapSource)
 * Role: The unified top-level factory entry point exposed to the Flink execution environment (env.fromSource).
 * Explicitly declared as bounded batch execution (Boundedness.BOUNDED).
 * Produces ImapSplitEnumerator (JobManager Master coordinator) and ImapSourceReader (TaskManager Worker).
 *
 * 💡【Architectural Note on Generics: Source<RawEmail, ImapSplit, Void>】
 * The Flink FLIP-27 Source interface defines three generic parameters: Source<T, SplitT, EnumChkT>
 * 1. T (RawEmail):
 *    The record type emitted by this Source and ingested by downstream Flink transformation operators.
 * 2. SplitT (ImapSplit):
 *    The task work-order descriptor (split) dispatched from Master (JobManager) to Worker (TaskManager).
 * 3. EnumChkT (java.lang.Void):
 *    The checkpoint state snapshot type persisted by SplitEnumerator during stateful fault-tolerance checkpoints.
 *    - Why capital 'Void' instead of lowercase 'void'?
 *      Java generics require reference object types (Classes). Lowercase 'void' is a language keyword 
 *      and forbidden inside generic angle brackets <...>. Capital 'java.lang.Void' is an uninstantiable 
 *      placeholder class whose only permissible value is null.
 *    - Why Void here?
 *      In our ephemeral run-to-completion batch architecture (Mode B), the ImapSplitEnumerator is purely 
 *      stateless across job invocations (snapshotState returns null). Specifying 'Void' is the standard 
 *      Flink paradigm to declare that no enumerator checkpointing state is managed or stored.
 */
public class ImapSource implements Source<RawEmail, ImapSplit, Void>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String proxyHost;
    private final int proxyPort;
    private final int maxBatchSize;
    private final ImapSyncMode syncMode;

    public ImapSource(String host, int port, String user, String password,
                      String proxyHost, int proxyPort, int maxBatchSize) {
        this(host, port, user, password, proxyHost, proxyPort, maxBatchSize, ImapSyncMode.EARLIEST_FIRST);
    }

    public ImapSource(String host, int port, String user, String password,
                      String proxyHost, int proxyPort, int maxBatchSize, ImapSyncMode syncMode) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.maxBatchSize = maxBatchSize;
        this.syncMode = syncMode != null ? syncMode : ImapSyncMode.EARLIEST_FIRST;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * 工厂方法：直接从 .env / 系统环境变量中装配标准 ImapSource
     */
    public static ImapSource fromConfig() {
        String host = ConfigUtils.get("GMAIL_IMAP_HOST", "imap.gmail.com");
        int port = ConfigUtils.getInt("GMAIL_IMAP_PORT", 993);
        String user = ConfigUtils.get("GMAIL_IMAP_USER", "alice.h.y.he@gmail.com");
        String password = ConfigUtils.get("GMAIL_IMAP_PASS", ConfigUtils.get("GMAIL_IMAP_PASSWORD", ""));
        String proxyHost = ConfigUtils.get("IMAP_PROXY_HOST");
        int proxyPort = ConfigUtils.getInt("IMAP_PROXY_PORT", 7890);
        int maxBatchSize = ConfigUtils.getInt("IMAP_MAX_BATCH_SIZE", 200);
        String syncModeStr = ConfigUtils.get("IMAP_SYNC_MODE", "EARLIEST_FIRST");
        ImapSyncMode syncMode = ImapSyncMode.fromString(syncModeStr);

        return new ImapSource(host, port, user, password, proxyHost, proxyPort, maxBatchSize, syncMode);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED; // 🎯 显式锁定为有界批处理数据源 (算完即焚)
    }

    @Override
    public SourceReader<RawEmail, ImapSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new ImapSourceReader(readerContext, host, port, user, password, proxyHost, proxyPort);
    }

    @Override
    public SplitEnumerator<ImapSplit, Void> createEnumerator(SplitEnumeratorContext<ImapSplit> enumContext) throws Exception {
        return new ImapSplitEnumerator(enumContext, host, port, user, password, proxyHost, proxyPort, maxBatchSize, syncMode);
    }

    @Override
    public SplitEnumerator<ImapSplit, Void> restoreEnumerator(SplitEnumeratorContext<ImapSplit> enumContext, Void checkpoint) throws Exception {
        return createEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<ImapSplit> getSplitSerializer() {
        return new ImapSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new VoidSerializer();
    }

    public String getUser() {
        return user;
    }

    public ImapSyncMode getSyncMode() {
        return syncMode;
    }

    /**
     * 空检查点序列化器
     */
    private static class VoidSerializer implements SimpleVersionedSerializer<Void>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Void obj) {
            return new byte[0];
        }

        @Override
        public Void deserialize(int version, byte[] serialized) {
            return null;
        }
    }

    /**
     * 链式参数构造器
     */
    public static class Builder {
        private String host = "imap.gmail.com";
        private int port = 993;
        private String user = "alice.h.y.he@gmail.com";
        private String password = "";
        private String proxyHost;
        private int proxyPort = 7890;
        private int maxBatchSize = 20;
        private ImapSyncMode syncMode = ImapSyncMode.EARLIEST_FIRST;

        public Builder host(String host) { this.host = host; return this; }
        public Builder port(int port) { this.port = port; return this; }
        public Builder user(String user) { this.user = user; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder proxy(String proxyHost, int proxyPort) {
            this.proxyHost = proxyHost;
            this.proxyPort = proxyPort;
            return this;
        }
        public Builder maxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; return this; }
        public Builder syncMode(ImapSyncMode syncMode) { this.syncMode = syncMode; return this; }

        public ImapSource build() {
            return new ImapSource(host, port, user, password, proxyHost, proxyPort, maxBatchSize, syncMode);
        }
    }
}
