package com.finance.etl.source.imap;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Flink FLIP-27 规范: IMAP 邮件分片任务工单 (ImapSplit)
 * 职责：描述单个 TaskManager Worker 负责读取的 IMAP 邮件范围（文件夹、UID 起止范围或具体未读 UID 列表）。
 * 不装载实际邮件正文，仅作为控制面调度指令载体。
 *
 * 💡【核心架构注解：Gmail 场景下的邮件 ID 维度与 UID 本质】
 * 在 Gmail / IMAP 体系中，存在 3 种完全不同维度的标识符，必须严格区分：
 * 1. RFC 3501 IMAP UID (32位无符号长整数，如 343, 352):
 *    - 归属：由 Gmail IMAP 服务器分配并在特定文件夹 (如 INBOX) 内保证单调递增且永久固定。
 *    - 用途：本工单中的 startUid、endUid 以及 specificUids 严格采用此 UID！
 *           它作为批处理增量同步的“水位线游标 (Offset Cursor)”，支持精准定位和范围拉取。
 *    - 警示：严禁使用 JavaMail 的 msg.getMessageNumber()（那只是易受删除扰动的易失临时数组下标）。
 * 2. RFC 2822 Message-ID (全局唯一字符串，如 <CAGH1234@mail.gmail.com>):
 *    - 归属：发信端邮件头唯一生成。
 *    - 用途：用于数据湖仓 ODS 表 (iceberg.finance.raw_sms_records) 的业务唯一防重键 (SHA-256 幂等指纹)。
 * 3. X-GM-MSGID (64位大整数):
 *    - 归属：Google 分布式底层数据库全局唯一编号，对应网页版 Gmail URL。
 */
public class ImapSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    /** 工单唯一流水号 (用于 Flink 引擎跟踪、失败重试与 Checkpoint 状态恢复) */
    private final String splitId;

    /** 目标邮件文件夹/标签 (例如 "INBOX") */
    private final String folderName;

    /** 连续切片起始 RFC 3501 IMAP UID (大规模全量历史回溯模式下作为低水位) */
    private final long startUid;

    /** 连续切片截止 RFC 3501 IMAP UID (大规模全量历史回溯模式下作为高水位) */
    private final long endUid;

    /** 离散未读 RFC 3501 IMAP UID 清单 (日常定时增量批处理核心字段，精准点播拉取) */
    private final List<Long> specificUids;

    /**
     * 构造函数 1：按具体 UID 列表分片 (最常用，精准拉取未读邮件)
     */
    public ImapSplit(String splitId, String folderName, List<Long> specificUids) {
        this.splitId = Objects.requireNonNull(splitId, "splitId must not be null");
        this.folderName = folderName != null ? folderName : "INBOX";
        this.specificUids = specificUids != null ? new ArrayList<>(specificUids) : Collections.emptyList();
        this.startUid = 0L;
        this.endUid = 0L;
    }

    /**
     * 构造函数 2：按 UID 起止范围分片 (用于大批量历史切片)
     */
    public ImapSplit(String splitId, String folderName, long startUid, long endUid) {
        this.splitId = Objects.requireNonNull(splitId, "splitId must not be null");
        this.folderName = folderName != null ? folderName : "INBOX";
        this.startUid = startUid;
        this.endUid = endUid;
        this.specificUids = Collections.emptyList();
    }

    /**
     * 构造函数 3：全量参数通用构造器
     */
    public ImapSplit(String splitId, String folderName, long startUid, long endUid, List<Long> specificUids) {
        this.splitId = Objects.requireNonNull(splitId, "splitId must not be null");
        this.folderName = folderName != null ? folderName : "INBOX";
        this.startUid = startUid;
        this.endUid = endUid;
        this.specificUids = specificUids != null ? new ArrayList<>(specificUids) : Collections.emptyList();
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getFolderName() {
        return folderName;
    }

    public long getStartUid() {
        return startUid;
    }

    public long getEndUid() {
        return endUid;
    }

    public List<Long> getSpecificUids() {
        return Collections.unmodifiableList(specificUids);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ImapSplit imapSplit = (ImapSplit) o;
        return startUid == imapSplit.startUid &&
                endUid == imapSplit.endUid &&
                Objects.equals(splitId, imapSplit.splitId) &&
                Objects.equals(folderName, imapSplit.folderName) &&
                Objects.equals(specificUids, imapSplit.specificUids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, folderName, startUid, endUid, specificUids);
    }

    @Override
    public String toString() {
        return "ImapSplit{" +
                "splitId='" + splitId + '\'' +
                ", folderName='" + folderName + '\'' +
                ", startUid=" + startUid +
                ", endUid=" + endUid +
                ", specificUidsCount=" + specificUids.size() +
                '}';
    }
}
