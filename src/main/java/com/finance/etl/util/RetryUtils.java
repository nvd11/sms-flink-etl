package com.finance.etl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 轻量级指数退避重试工具 (RetryUtils)
 * 职责：为瞬态网络/DNS/连接抖动提供统一的重试语义，避免一次解析失败就葬送整个批处理作业。
 */
public final class RetryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);

    private RetryUtils() {
        // 工具类私有构造，禁止实例化
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    /**
     * 执行带指数退避的重试动作。
     *
     * @param action         动作名称（用于日志定位）
     * @param maxAttempts    最大尝试次数（含首次）
     * @param initialDelayMs 首次重试前等待毫秒数，之后每次翻倍，上限 30 秒
     * @param supplier       实际业务动作
     * @return 首次成功的结果
     * @throws Exception 全部尝试失败后抛出最后一次异常
     */
    public static <T> T retry(String action, int maxAttempts, long initialDelayMs,
                              CheckedSupplier<T> supplier) throws Exception {
        int attempts = Math.max(1, maxAttempts);
        long delay = Math.max(200L, initialDelayMs);
        Exception last = null;

        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                return supplier.get();
            } catch (Exception e) {
                last = e;
                if (isNonRetryable(e) || attempt >= attempts) {
                    break;
                }
                LOG.warn("⚠️ [Retry] {} failed on attempt {}/{} ({}). Retrying in {} ms...",
                        action, attempt, attempts, e.getMessage(), delay);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
                delay = Math.min(delay * 2L, 30_000L);
            }
        }

        LOG.error("❌ [Retry] {} exhausted all {} attempt(s): {}",
                action, attempts, last != null ? last.getMessage() : "unknown");
        throw last != null ? last : new IllegalStateException("Retry failed without exception: " + action);
    }

    /**
     * 表不存在属于确定性错误，重试无意义，立即向上抛出以免白等退避时间。
     */
    private static boolean isNonRetryable(Throwable e) {
        String name = e.getClass().getName();
        return name.endsWith("NoSuchTableException") || name.endsWith("TableNotFoundException");
    }
}
