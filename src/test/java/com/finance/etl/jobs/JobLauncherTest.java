package com.finance.etl.jobs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("JobLauncher 路由引导程序单元测试")
class JobLauncherTest {

    @Test
    @DisplayName("测试传入非法 job target 时安全抛出异常")
    void testInvalidTargetThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            JobLauncher.main(new String[]{"invalid-job-name"});
        });
    }
}
