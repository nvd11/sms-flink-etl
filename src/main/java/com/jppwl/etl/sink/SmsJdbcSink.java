package com.jppwl.etl.sink;

import com.jppwl.etl.model.SmsRecord;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

public class SmsJdbcSink {

    private static final String UPSERT_SQL =
            "INSERT INTO raw_sms_records (" +
            "  email_uid, source_type, sender, received_at, raw_subject, raw_content, " +
            "  sms_category, parsed_amount, parsed_currency, parsed_card_no, parsed_merchant, created_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "  sms_category = VALUES(sms_category), " +
            "  parsed_amount = VALUES(parsed_amount), " +
            "  parsed_card_no = VALUES(parsed_card_no), " +
            "  parsed_merchant = VALUES(parsed_merchant)";

    public static SinkFunction<SmsRecord> createSink(String url, String username, String password) {
        return JdbcSink.sink(
                UPSERT_SQL,
                (statement, record) -> {
                    statement.setString(1, record.getEmailUid());
                    statement.setString(2, record.getSourceType());
                    statement.setString(3, record.getSender());
                    statement.setTimestamp(4, record.getReceivedAt() != null ? Timestamp.valueOf(record.getReceivedAt()) : new Timestamp(System.currentTimeMillis()));
                    statement.setString(5, record.getRawSubject());
                    statement.setString(6, record.getRawContent());
                    statement.setString(7, record.getSmsCategory());
                    statement.setBigDecimal(8, record.getParsedAmount());
                    statement.setString(9, record.getParsedCurrency());
                    statement.setString(10, record.getParsedCardNo());
                    statement.setString(11, record.getParsedMerchant());
                    statement.setTimestamp(12, record.getCreatedAt() != null ? Timestamp.valueOf(record.getCreatedAt()) : new Timestamp(System.currentTimeMillis()));
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(10)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );
    }
}
