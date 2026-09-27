package com.finance.etl.util;

import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统一环境变量与敏感配置加载器 (类似 Python 的 load_dotenv)
 * 优先级: 操作系统真实环境变量 (生产 K8s Secret) > 本地 .env 文件 (本地调试) > 默认保底值
 */
public class ConfigUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

    private static final Dotenv DOTENV;

    static {
        Dotenv loaded = null;
        try {
            // 自动检索当前目录及父目录的 .env 文件，若缺失则静默忽略 (适合生产无文件纯环境变量容器)
            loaded = Dotenv.configure()
                    .ignoreIfMissing()
                    .systemProperties()
                    .load();
            LOG.info("🌱 [ConfigUtils] Loaded environment configuration from .env file successfully.");
        } catch (Exception e) {
            LOG.warn("⚠️ [ConfigUtils] .env file not found or failed to parse, fallback to OS environment only.");
        }
        DOTENV = loaded;
    }

    /**
     * 获取指定配置项的值 (优先读取 OS 环境变量，其次读取 .env，最后为空)
     */
    public static String get(String key) {
        return get(key, null);
    }

    /**
     * 获取指定配置项的值，支持提供默认保底值
     */
    public static String get(String key, String defaultValue) {
        // 1. 优先读取操作系统真实环境变量 (适配 K8s Pod 注入)
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.trim().isEmpty()) {
            return envVal.trim();
        }

        // 2. 次优读取 .env 文件中的值 (适配本地单元测试与调试)
        if (DOTENV != null) {
            String dotVal = DOTENV.get(key);
            if (dotVal != null && !dotVal.trim().isEmpty()) {
                return dotVal.trim();
            }
        }

        // 3. 返回默认保底值
        return defaultValue;
    }

    /**
     * 获取整型配置值
     */
    public static int getInt(String key, int defaultValue) {
        String val = get(key);
        if (val == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取布尔型配置值
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(val.trim());
    }
}
