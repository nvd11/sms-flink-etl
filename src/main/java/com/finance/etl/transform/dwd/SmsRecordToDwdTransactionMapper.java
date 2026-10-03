package com.finance.etl.transform.dwd;

import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.model.SmsRecord;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DWD 领域模型转换算子 (SmsRecordToDwdTransactionMapper)
 * 职责：
 * 编排组装所有单一职责提取器 (DwdFieldExtractor)，将 ODS 的 SmsRecord 领域实体转换为
 * DWD 层规范化的 FinancialTransaction 动账事实实体。
 * 并内置分布式最大 ID 累加器，支撑端到端断点续传水位跟踪。
 */
public class SmsRecordToDwdTransactionMapper extends RichMapFunction<SmsRecord, FinancialTransaction> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SmsRecordToDwdTransactionMapper.class);

    public static final String ACCUMULATOR_MAX_RECORD_ID = "max-processed-record-id";
    private final LongMaximum maxRecordIdTracker = new LongMaximum();

    private final ValidTxExtractor validTxExtractor;
    private final AmountExtractor amountExtractor;
    private final TxTypeExtractor txTypeExtractor;
    private final AccountExtractor accountExtractor;
    private final PaymentChannelExtractor paymentChannelExtractor;
    private final TxTimeExtractor txTimeExtractor;
    private final MerchantExtractor merchantExtractor;
    private final CategoryExtractor categoryExtractor;

    public SmsRecordToDwdTransactionMapper() {
        this.validTxExtractor = new ValidTxExtractor();
        this.amountExtractor = new AmountExtractor();
        this.txTypeExtractor = new TxTypeExtractor();
        this.accountExtractor = new AccountExtractor();
        this.paymentChannelExtractor = new PaymentChannelExtractor();
        this.txTimeExtractor = new TxTimeExtractor();
        this.merchantExtractor = new MerchantExtractor();
        this.categoryExtractor = new CategoryExtractor();
    }

    public SmsRecordToDwdTransactionMapper(ValidTxExtractor validTxExtractor,
                                          AmountExtractor amountExtractor,
                                          TxTypeExtractor txTypeExtractor,
                                          AccountExtractor accountExtractor,
                                          PaymentChannelExtractor paymentChannelExtractor,
                                          TxTimeExtractor txTimeExtractor,
                                          MerchantExtractor merchantExtractor,
                                          CategoryExtractor categoryExtractor) {
        this.validTxExtractor = Objects.requireNonNull(validTxExtractor);
        this.amountExtractor = Objects.requireNonNull(amountExtractor);
        this.txTypeExtractor = Objects.requireNonNull(txTypeExtractor);
        this.accountExtractor = Objects.requireNonNull(accountExtractor);
        this.paymentChannelExtractor = Objects.requireNonNull(paymentChannelExtractor);
        this.txTimeExtractor = Objects.requireNonNull(txTimeExtractor);
        this.merchantExtractor = Objects.requireNonNull(merchantExtractor);
        this.categoryExtractor = Objects.requireNonNull(categoryExtractor);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(ACCUMULATOR_MAX_RECORD_ID, maxRecordIdTracker);
    }

    @Override
    public FinancialTransaction map(SmsRecord record) throws Exception {
        if (record == null) {
            return null;
        }

        // 统计最大 recordId (用于作业完成后推进水位)
        if (record.getId() != null) {
            maxRecordIdTracker.add(record.getId());
        }

        // 1. 提取各个业务维度
        Map<String, Object> validRes = validTxExtractor.extract(record);
        Boolean isValidTx = (Boolean) validRes.getOrDefault("is_valid_tx", false);

        Map<String, Object> amountRes = amountExtractor.extract(record);
        BigDecimal amount = (BigDecimal) amountRes.get("amount");
        String currency = (String) amountRes.getOrDefault("currency", "CNY");

        Map<String, Object> txTypeRes = txTypeExtractor.extract(record);
        String direction = (String) txTypeRes.get("direction");
        String txType = (String) txTypeRes.get("tx_type");

        Map<String, Object> accountRes = accountExtractor.extract(record);
        String cardTail = (String) accountRes.get("card_tail");
        String accountType = (String) accountRes.get("account_type");

        Map<String, Object> channelRes = paymentChannelExtractor.extract(record);
        String paymentChannel = (String) channelRes.get("payment_channel");

        Map<String, Object> timeRes = txTimeExtractor.extract(record);
        Instant txTime = (Instant) timeRes.get("tx_time");
        if (txTime == null) {
            txTime = record.getReceivedAt() != null ? record.getReceivedAt() : Instant.now();
        }

        Map<String, Object> merchantRes = merchantExtractor.extract(record);
        String counterparty = (String) merchantRes.get("counterparty");
        String cleanedMerchant = (String) merchantRes.get("cleaned_merchant");

        Map<String, Object> categoryRes = categoryExtractor.extract(record);
        String category = (String) categoryRes.get("category");

        // 2. 生成动账事实唯一标识 (txId: 格式为 tx_<rawRecordId>_<timestampMs>)
        String txId = String.format("tx_%d_%d",
                record.getId() != null ? record.getId() : 0L,
                txTime.toEpochMilli());

        String institution = record.getSender();

        Instant etlCreatedAt = Instant.now();

        FinancialTransaction tx = new FinancialTransaction(
                txId,
                record.getId(),
                txTime,
                amount,
                currency,
                direction,
                txType,
                institution,
                accountType,
                cardTail,
                paymentChannel,
                counterparty,
                cleanedMerchant,
                category,
                isValidTx,
                etlCreatedAt
        );

        LOG.debug("✨ [DwdMapper] Converted ID: {} => txId: {}, isValid: {}, amount: {} {}, type: {}",
                record.getId(), txId, isValidTx, amount, currency, txType);

        return tx;
    }
}
