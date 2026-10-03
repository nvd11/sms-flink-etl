package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TxTypeExtractor 交易类型与资金流向提取器单元测试")
class TxTypeExtractorTest {

    private TxTypeExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new TxTypeExtractor();
    }

    private SmsRecord createRecord(String rawBody) {
        return new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), rawBody, Instant.now());
    }

    @ParameterizedTest(name = "[{index}] 交易细分类型提取: {0} => dir: {1}, type: {2}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡03日10:58消费14.89人民币，交易商户:支付宝-高德打车。', OUTFLOW, EXPENSE",
            "'106980095508【广发银行】您尾号3342信用卡09月27日发起退款人民币43.92元，到账情况点查询', INFLOW, REFUND",
            "'10690661440018【中意人寿】尊敬的潘瑞成：您的理赔申请已通过审核，赔款金额110.79元将于0-5个工作日到账', INFLOW, INCOME",
            "'106980095508【广发银行】您尾号3342信用卡16日00:43还款人民币17253.33元，到账后0', OUTFLOW, TRANSFER",
            "'10692576032【平安产险】尊敬的潘文林，您已付6646.00元的保单已承保', OUTFLOW, EXPENSE",
            "'【微信支付】微信零钱已向某某便利店成功付款8.00元。SubId：1', OUTFLOW, EXPENSE",
            "'【支付宝】花呗自动扣款通知：扣款成功299.00元。SubId：2', OUTFLOW, EXPENSE"
    })
    void testTxTypeAndDirection(String rawBody, String expectedDirection, String expectedType) {
        Map<String, Object> result = extractor.extract(createRecord(rawBody));

        assertEquals(expectedDirection, result.get("direction"));
        assertEquals(expectedType, result.get("tx_type"));
    }

    @Test
    @DisplayName("测试非交易通知安全返回空 Map")
    void testIgnoredNoticesReturnEmpty() {
        String[] samples = {
                "106980095188000001【支付宝】支付宝验证码：283094，请勿向他人泄露您的验证码！",
                "106910095366【汇丰银行中国】温馨提示：您尾号0025的美元信用卡当月账单为5.99元，最低还款额为0.30元",
                "com.tencent.mm广发信用卡: 交易成功提醒服务号UID：103362026-10-03 10:58:33",
                "95566您在我行住房贷款的供款账户余额不足，请补充账户余额"
        };

        for (String sample : samples) {
            Map<String, Object> result = extractor.extract(createRecord(sample));
            assertTrue(result.isEmpty(), "非动账短信不得提取出交易类型: " + sample);
        }
    }

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量审计资金方向与交易类型分布")
    void testExtractAllSmsFromDevTable() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        int typedCount = 0;
        int untypedCount = 0;
        Map<String, Integer> directionCounts = new HashMap<>();
        Map<String, Integer> txTypeCounts = new HashMap<>();
        List<String> allTypedLogs = new ArrayList<>();
        List<String> allUntypedLogs = new ArrayList<>();

        System.out.println("================================================================================");
        System.out.println("📊 [Dev Lakehouse TxType Audit] Auditing direction and tx_type across all ODS records...");
        System.out.println("================================================================================");

        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> records =
                     org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
            for (org.apache.iceberg.data.Record r : records) {
                totalCount++;
                Long id = r.get(0, Long.class);
                String msgUid = r.get(1, String.class);
                String channel = r.get(2, String.class);
                String sender = r.get(3, String.class);
                String receiverPhone = r.get(4, String.class);
                String rawBody = r.get(6, String.class);

                SmsRecord sms = new SmsRecord(id, msgUid, channel, sender, receiverPhone, null, rawBody, null);
                Map<String, Object> result = extractor.extract(sms);

                if (!result.isEmpty()) {
                    typedCount++;
                    String direction = (String) result.get("direction");
                    String txType = (String) result.get("tx_type");

                    assertNotNull(direction, "提取的资金方向不可为 null");
                    assertNotNull(txType, "提取的交易类型不可为 null");

                    directionCounts.put(direction, directionCounts.getOrDefault(direction, 0) + 1);
                    txTypeCounts.put(txType, txTypeCounts.getOrDefault(txType, 0) + 1);

                    allTypedLogs.add(String.format("[TYPED #%3d | ID:%3d | %-4s] => %-7s | %-8s | %s",
                            typedCount, id, sender != null ? sender : "N/A", direction, txType, rawBody));
                } else {
                    untypedCount++;
                    allUntypedLogs.add(String.format("[UNTYPED #%3d | ID:%3d | %-4s] | %s",
                            untypedCount, id, sender != null ? sender : "N/A", rawBody));
                }
            }
        }

        // 保存全部详细日志供深度审计
        java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/opencode/txtype_audit_all.txt"), allTypedLogs);
        java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/opencode/txtype_audit_untyped.txt"), allUntypedLogs);

        System.out.println("================================================================================");
        System.out.println("📈 [TxType Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS)       : %d 封\n", totalCount);
        System.out.printf("  • 识别出交易类型 (Typed Count)   : %d 笔\n", typedCount);
        System.out.printf("  • 未识别/被过滤  (Untyped Count) : %d 封\n", untypedCount);
        System.out.println("  • 资金方向分布 (Direction)       : " + directionCounts);
        System.out.println("  • 细分类型分布 (Transaction Type): " + txTypeCounts);
        System.out.println("================================================================================");

        assertTrue(totalCount > 0, "Dev 表中必须有数据可供审计");
        assertTrue(typedCount > 0, "必须成功识别出交易类型");
        assertTrue(directionCounts.containsKey("OUTFLOW"), "必须包含支出流向");
        assertTrue(directionCounts.containsKey("INFLOW"), "必须包含流入流向");
    }
}
