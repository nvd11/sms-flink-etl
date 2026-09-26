# 基础镜像使用官方轻量 JRE 21 (Ubuntu Jammy)
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# 安装必要的时区与证书组件
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai

# 关联开源 GitHub 仓库
LABEL org.opencontainers.image.source="https://github.com/nvd11/sms-flink-etl"
LABEL org.opencontainers.image.description="HelloWorld Flink Batch Job Image for Cluster Health Checks"
LABEL org.opencontainers.image.licenses="MIT"

# 复制由 CI 编译生成的 Fat JAR
COPY target/sms-flink-etl-1.0.0.jar /app/sms-flink-etl-1.0.0.jar

# 非 root 安全用户运行
USER 1000:1000

# 纯批处理算完即焚入口 (全面挂载 JDK 21 反射通道与内存限额)
ENTRYPOINT ["java", \
    "--add-opens=java.base/java.lang=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
    "--add-opens=java.base/java.util=ALL-UNNAMED", \
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
    "-Xmx1536m", \
    "-jar", "/app/sms-flink-etl-1.0.0.jar"]
