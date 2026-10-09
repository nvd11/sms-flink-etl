# 基础镜像使用官方轻量 JRE 21 (Ubuntu Jammy)
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# 安装必要的时区与证书组件
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai
ENV HADOOP_USER_NAME=flink

# 创建明确的非 root 用户 flink (解决 Hadoop UserGroupInformation UnixPrincipal 空指针异常)
RUN groupadd -g 1000 flink && useradd -u 1000 -g flink -m -s /bin/bash flink

# 关联开源 GitHub 仓库
LABEL org.opencontainers.image.source="https://github.com/nvd11/sms-flink-etl"
LABEL org.opencontainers.image.description="SMS Flink ETL Lakehouse Batch Job"
LABEL org.opencontainers.image.licenses="MIT"

# 复制由 CI 编译生成的 Fat JAR
COPY target/sms-flink-etl-1.0.0.jar /app/sms-flink-etl-1.0.0.jar
RUN chown -R flink:flink /app

# 非 root 安全用户运行
USER flink

# 纯批处理算完即焚入口 (全面挂载 JDK 21 反射通道与内存限额，显式传递 user.name 杜绝 Hadoop UGI 认证空指针)
ENTRYPOINT ["java", \
    "-Duser.name=flink", \
    "-DHADOOP_USER_NAME=flink", \
    "--add-opens=java.base/java.lang=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
    "--add-opens=java.base/java.util=ALL-UNNAMED", \
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", \
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED", \
    "--add-opens=java.base/java.net=ALL-UNNAMED", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--add-opens=java.base/java.time=ALL-UNNAMED", \
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
    "-Xmx1536m", \
    "-jar", "/app/sms-flink-etl-1.0.0.jar"]
