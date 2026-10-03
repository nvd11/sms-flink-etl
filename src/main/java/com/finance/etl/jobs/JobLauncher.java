package com.finance.etl.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * 通用作业调度入口引导程序 (JobLauncher)
 * 职责：
 * 作为 Docker 容器镜像的单一 ENTRYPOINT 入口。
 * 支持根据传入的第一个参数 (或环境变量 FLINK_JOB_NAME) 动态分发并拉起具体的主作业：
 * - "ods" 或 "SmsGmailR2Job" -> 执行 com.finance.etl.jobs.SmsGmailR2Job
 * - "dwd" 或 "SmsOdsToDwdJob" -> 执行 com.finance.etl.jobs.SmsOdsToDwdJob
 * - 默认 (无参数) -> 执行 SmsGmailR2Job (保持向下兼容)
 */
public class JobLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(JobLauncher.class);

    public static void main(String[] args) throws Exception {
        String targetJob = null;
        String[] forwardedArgs = new String[0];

        if (args != null && args.length > 0) {
            targetJob = args[0];
            forwardedArgs = Arrays.copyOfRange(args, 1, args.length);
        }

        if (targetJob == null || targetJob.trim().isEmpty()) {
            targetJob = System.getenv("FLINK_JOB_NAME");
        }

        if (targetJob == null || targetJob.trim().isEmpty()) {
            targetJob = "ods"; // 默认缺省值为 ods 入湖
        }

        targetJob = targetJob.trim().toLowerCase();
        LOG.info("🎯 [JobLauncher] Launching target batch job: '{}' with forwarded args: {}",
                targetJob, Arrays.toString(forwardedArgs));

        switch (targetJob) {
            case "ods":
            case "smsgmailr2job":
            case "sms-gmail-r2":
                LOG.info("🚀 Invoking SmsGmailR2Job.main()...");
                SmsGmailR2Job.main(forwardedArgs);
                break;

            case "dwd":
            case "smsodstodwdjob":
            case "sms-ods-to-dwd":
                LOG.info("🚀 Invoking SmsOdsToDwdJob.main()...");
                SmsOdsToDwdJob.main(forwardedArgs);
                break;

            default:
                LOG.error("❌ Unknown job name: '{}'. Supported targets: ['ods', 'dwd']", targetJob);
                throw new IllegalArgumentException("Unknown job target: " + targetJob + ". Expected 'ods' or 'dwd'");
        }
    }
}
