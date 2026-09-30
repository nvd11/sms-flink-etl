package com.finance.etl.source.imap;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * IMAP 协议底层网络与连接辅助工具类 (ImapUtils)
 * 职责：专职负责 JavaMail IMAP/IMAPS 协议层的连接配置组装与网络辅助逻辑，
 * 供 ImapSplitEnumerator (Master) 与 ImapSourceReader (Worker) 共享复用，
 * 避免网络配置重复编写，保持上层门面类 (ImapSource) 的纯粹性。
 */
public final class ImapUtils {

    private ImapUtils() {
        // 工具类私有构造，禁止实例化
    }

    /**
     * 统一构建 JavaMail IMAPS 核心连接配置 (TLS 加密与 SOCKS5 代理)
     */
    public static Properties createImapsProperties(String host, int port, @Nullable String proxyHost, int proxyPort) {
        Properties props = new Properties();
        props.put("mail.store.protocol", "imaps");
        props.put("mail.imaps.host", host);
        props.put("mail.imaps.port", String.valueOf(port));
        props.put("mail.imaps.ssl.enable", "true");
        props.put("mail.imaps.connectiontimeout", "10000");
        props.put("mail.imaps.timeout", "10000");

        if (proxyHost != null && !proxyHost.trim().isEmpty()) {
            props.put("mail.imaps.socks.host", proxyHost.trim());
            props.put("mail.imaps.socks.port", String.valueOf(proxyPort));
        }
        return props;
    }
}
