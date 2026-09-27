package com.finance.etl.source.imap;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink FLIP-27 规范: IMAP 任务工单版本化二进制编解码器 (ImapSplitSerializer)
 * 职责：负责在 Master (JobManager) 与 Worker (TaskManager) 跨网络通信以及 Checkpoint 存档时，
 * 将 ImapSplit 工单压缩为紧凑的二进制字节流，或从字节流中安全反序列化还原。
 */
public class ImapSplitSerializer implements SimpleVersionedSerializer<ImapSplit> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ImapSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            // 1. 编码工单 ID 与文件夹名
            out.writeUTF(split.splitId());
            out.writeUTF(split.getFolderName());

            // 2. 编码起止 UID 范围
            out.writeLong(split.getStartUid());
            out.writeLong(split.getEndUid());

            // 3. 编码离散未读 UID 列表
            List<Long> uids = split.getSpecificUids();
            out.writeInt(uids.size());
            for (Long uid : uids) {
                out.writeLong(uid);
            }
            out.flush();
        }
        return baos.toByteArray();
    }

    @Override
    public ImapSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(String.format(
                    "Unsupported version [%d] for ImapSplitSerializer. Current supported version is [%d]",
                    version, CURRENT_VERSION));
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String splitId = in.readUTF();
            String folderName = in.readUTF();
            long startUid = in.readLong();
            long endUid = in.readLong();

            int size = in.readInt();
            List<Long> specificUids = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                specificUids.add(in.readLong());
            }

            return new ImapSplit(splitId, folderName, startUid, endUid, specificUids);
        }
    }
}
