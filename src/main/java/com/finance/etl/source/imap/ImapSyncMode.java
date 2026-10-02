package com.finance.etl.source.imap;

import java.io.Serializable;

/**
 * IMAP 邮件消费切片模式 (ImapSyncMode)
 * - EARLIEST_FIRST (默认 · 正向全量补录):
 *   冷启动时从第 1 封历史邮件开始正向抽取，随着定时任务轮询平稳追赶历史，一封不漏；
 * - LATEST_FIRST (倒序最新窗口优先):
 *   冷启动时直接从收件箱最新尾部倒序截取一个窗口，优先保证近端账目可用。
 */
public enum ImapSyncMode implements Serializable {
    EARLIEST_FIRST,
    LATEST_FIRST;

    public static ImapSyncMode fromString(String val) {
        if (val != null) {
            String clean = val.trim().toUpperCase();
            if (clean.equals("LATEST") || clean.equals("LATEST_FIRST") || clean.equals("LATEST_WINDOW")) {
                return LATEST_FIRST;
            }
        }
        return EARLIEST_FIRST;
    }
}
