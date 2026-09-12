FROM flink:1.19.0-java17

LABEL maintainer="Jason <jason1.pan@hsbc.com.hk>"
LABEL description="Java Flink on K3s SMS and Bookkeeping ETL Pipeline"

# Create user library directory
RUN mkdir -p /opt/flink/usrlib

# Copy shaded fat jar
COPY target/sms-flink-etl-1.0.0.jar /opt/flink/usrlib/sms-flink-etl.jar

USER flink
WORKDIR /opt/flink
