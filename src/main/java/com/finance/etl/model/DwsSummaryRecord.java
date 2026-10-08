package com.finance.etl.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;

/**
 * 湖仓 DWS 财务聚合统计行领域实体 (DwsSummaryRecord)
 * 职责：
 * 承载来自 Trino DWS 视图 (daily / weekly / monthly) 抽取的宏观大盘统计行。
 * 覆盖 11 大细分生活消费类目，严密平账且自带增量水位。
 */
public class DwsSummaryRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String periodType; // "DAILY", "WEEKLY", "MONTHLY"
    private String periodValue; // e.g. "2026-10-03", "2026-W40", "2026-09"
    private LocalDate statDate; // 日维度专用
    private Long minId; // 包含的最小 DWD 行号
    private Long maxId; // 包含的最大 DWD 行号 (增量游标)
    private Long txCount; // 有效消费笔数

    // 核心财务总览指标
    private BigDecimal totalExpense; // 人民币总消费支出 (不含退款冲正)
    private BigDecimal totalRefund; // 退款总额
    private BigDecimal netExpense; // 净消费支出 (totalExpense - totalRefund)
    private BigDecimal totalIncome; // 理赔到账等被动收入总额
    private BigDecimal totalTransfer; // 信用卡还款划转/内部转账总额

    // 11 大细分消费类目开销
    private BigDecimal foodExpense; // 餐饮美食开销 (FOOD)
    private BigDecimal transportExpense; // 交通出行打车开销 (TRANSPORT)
    private BigDecimal onlineShoppingExpense; // 线上电商网购开销 (ONLINE_SHOPPING)
    private BigDecimal offlineShoppingExpense; // 线下实体商超开销 (OFFLINE_SHOPPING)
    private BigDecimal medicalExpense; // 医疗健康药品支出 (MEDICAL)
    private BigDecimal communicationExpense; // 电信通信话费宽带 (COMMUNICATION)
    private BigDecimal insuranceExpense; // 保险保费支出 (INSURANCE)
    private BigDecimal propertyExpense; // 物业管理费支出 (PROPERTY_MANAGEMENT)
    private BigDecimal travelExpense; // 旅游文旅度假支出 (TRAVEL)
    private BigDecimal personalTransferExpense; // 个人转账扫码支出 (PERSONAL_TRANSFER)
    private BigDecimal otherExpense; // 其他平台杂项支出 (OTHER)

    // 单笔峰值与主力商户
    private BigDecimal maxSingleAmount; // 单笔最大支出
    private String maxMerchant; // 单笔最大支出商户

    public DwsSummaryRecord() {
    }

    public DwsSummaryRecord(String periodType, String periodValue, LocalDate statDate, Long minId, Long maxId, Long txCount,
                            BigDecimal totalExpense, BigDecimal totalRefund, BigDecimal netExpense,
                            BigDecimal totalIncome, BigDecimal totalTransfer,
                            BigDecimal foodExpense, BigDecimal transportExpense,
                            BigDecimal onlineShoppingExpense, BigDecimal offlineShoppingExpense,
                            BigDecimal medicalExpense, BigDecimal communicationExpense,
                            BigDecimal insuranceExpense, BigDecimal propertyExpense,
                            BigDecimal travelExpense, BigDecimal personalTransferExpense,
                            BigDecimal otherExpense,
                            BigDecimal maxSingleAmount, String maxMerchant) {
        this.periodType = periodType;
        this.periodValue = periodValue;
        this.statDate = statDate;
        this.minId = minId;
        this.maxId = maxId;
        this.txCount = txCount;
        this.totalExpense = defaultZero(totalExpense);
        this.totalRefund = defaultZero(totalRefund);
        this.netExpense = defaultZero(netExpense);
        this.totalIncome = defaultZero(totalIncome);
        this.totalTransfer = defaultZero(totalTransfer);
        this.foodExpense = defaultZero(foodExpense);
        this.transportExpense = defaultZero(transportExpense);
        this.onlineShoppingExpense = defaultZero(onlineShoppingExpense);
        this.offlineShoppingExpense = defaultZero(offlineShoppingExpense);
        this.medicalExpense = defaultZero(medicalExpense);
        this.communicationExpense = defaultZero(communicationExpense);
        this.insuranceExpense = defaultZero(insuranceExpense);
        this.propertyExpense = defaultZero(propertyExpense);
        this.travelExpense = defaultZero(travelExpense);
        this.personalTransferExpense = defaultZero(personalTransferExpense);
        this.otherExpense = defaultZero(otherExpense);
        this.maxSingleAmount = defaultZero(maxSingleAmount);
        this.maxMerchant = maxMerchant;
    }

    private static BigDecimal defaultZero(BigDecimal val) {
        return val != null ? val : BigDecimal.ZERO;
    }

    public String getPeriodType() {
        return periodType;
    }

    public void setPeriodType(String periodType) {
        this.periodType = periodType;
    }

    public String getPeriodValue() {
        return periodValue;
    }

    public void setPeriodValue(String periodValue) {
        this.periodValue = periodValue;
    }

    public LocalDate getStatDate() {
        return statDate;
    }

    public void setStatDate(LocalDate statDate) {
        this.statDate = statDate;
    }

    public Long getMinId() {
        return minId;
    }

    public void setMinId(Long minId) {
        this.minId = minId;
    }

    public Long getMaxId() {
        return maxId;
    }

    public void setMaxId(Long maxId) {
        this.maxId = maxId;
    }

    public Long getTxCount() {
        return txCount;
    }

    public void setTxCount(Long txCount) {
        this.txCount = txCount;
    }

    public BigDecimal getTotalExpense() {
        return totalExpense;
    }

    public void setTotalExpense(BigDecimal totalExpense) {
        this.totalExpense = defaultZero(totalExpense);
    }

    public BigDecimal getTotalRefund() {
        return totalRefund;
    }

    public void setTotalRefund(BigDecimal totalRefund) {
        this.totalRefund = defaultZero(totalRefund);
    }

    public BigDecimal getNetExpense() {
        return netExpense;
    }

    public void setNetExpense(BigDecimal netExpense) {
        this.netExpense = defaultZero(netExpense);
    }

    public BigDecimal getTotalIncome() {
        return totalIncome;
    }

    public void setTotalIncome(BigDecimal totalIncome) {
        this.totalIncome = defaultZero(totalIncome);
    }

    public BigDecimal getTotalTransfer() {
        return totalTransfer;
    }

    public void setTotalTransfer(BigDecimal totalTransfer) {
        this.totalTransfer = defaultZero(totalTransfer);
    }

    public BigDecimal getFoodExpense() {
        return foodExpense;
    }

    public void setFoodExpense(BigDecimal foodExpense) {
        this.foodExpense = defaultZero(foodExpense);
    }

    public BigDecimal getTransportExpense() {
        return transportExpense;
    }

    public void setTransportExpense(BigDecimal transportExpense) {
        this.transportExpense = defaultZero(transportExpense);
    }

    public BigDecimal getOnlineShoppingExpense() {
        return onlineShoppingExpense;
    }

    public void setOnlineShoppingExpense(BigDecimal onlineShoppingExpense) {
        this.onlineShoppingExpense = defaultZero(onlineShoppingExpense);
    }

    public BigDecimal getOfflineShoppingExpense() {
        return offlineShoppingExpense;
    }

    public void setOfflineShoppingExpense(BigDecimal offlineShoppingExpense) {
        this.offlineShoppingExpense = defaultZero(offlineShoppingExpense);
    }

    public BigDecimal getMedicalExpense() {
        return medicalExpense;
    }

    public void setMedicalExpense(BigDecimal medicalExpense) {
        this.medicalExpense = defaultZero(medicalExpense);
    }

    public BigDecimal getCommunicationExpense() {
        return communicationExpense;
    }

    public void setCommunicationExpense(BigDecimal communicationExpense) {
        this.communicationExpense = defaultZero(communicationExpense);
    }

    public BigDecimal getInsuranceExpense() {
        return insuranceExpense;
    }

    public void setInsuranceExpense(BigDecimal insuranceExpense) {
        this.insuranceExpense = defaultZero(insuranceExpense);
    }

    public BigDecimal getPropertyExpense() {
        return propertyExpense;
    }

    public void setPropertyExpense(BigDecimal propertyExpense) {
        this.propertyExpense = defaultZero(propertyExpense);
    }

    public BigDecimal getTravelExpense() {
        return travelExpense;
    }

    public void setTravelExpense(BigDecimal travelExpense) {
        this.travelExpense = defaultZero(travelExpense);
    }

    public BigDecimal getPersonalTransferExpense() {
        return personalTransferExpense;
    }

    public void setPersonalTransferExpense(BigDecimal personalTransferExpense) {
        this.personalTransferExpense = defaultZero(personalTransferExpense);
    }

    public BigDecimal getOtherExpense() {
        return otherExpense;
    }

    public void setOtherExpense(BigDecimal otherExpense) {
        this.otherExpense = defaultZero(otherExpense);
    }

    public BigDecimal getMaxSingleAmount() {
        return maxSingleAmount;
    }

    public void setMaxSingleAmount(BigDecimal maxSingleAmount) {
        this.maxSingleAmount = defaultZero(maxSingleAmount);
    }

    public String getMaxMerchant() {
        return maxMerchant;
    }

    public void setMaxMerchant(String maxMerchant) {
        this.maxMerchant = maxMerchant;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DwsSummaryRecord that = (DwsSummaryRecord) o;
        return Objects.equals(periodType, that.periodType) &&
                Objects.equals(periodValue, that.periodValue) &&
                Objects.equals(maxId, that.maxId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(periodType, periodValue, maxId);
    }

    public String toJson() {
        return String.format(
                "{\"periodType\":\"%s\",\"periodValue\":\"%s\",\"txCount\":%s,\"totalExpense\":%s,\"totalRefund\":%s,\"netExpense\":%s,\"totalIncome\":%s,\"totalTransfer\":%s,\"foodExpense\":%s,\"transportExpense\":%s,\"onlineShoppingExpense\":%s,\"offlineShoppingExpense\":%s,\"medicalExpense\":%s,\"communicationExpense\":%s,\"insuranceExpense\":%s,\"propertyExpense\":%s,\"travelExpense\":%s,\"personalTransferExpense\":%s,\"otherExpense\":%s}",
                periodType != null ? periodType : "",
                periodValue != null ? periodValue : "",
                txCount != null ? txCount : 0,
                totalExpense != null ? totalExpense : 0,
                totalRefund != null ? totalRefund : 0,
                netExpense != null ? netExpense : 0,
                totalIncome != null ? totalIncome : 0,
                totalTransfer != null ? totalTransfer : 0,
                foodExpense != null ? foodExpense : 0,
                transportExpense != null ? transportExpense : 0,
                onlineShoppingExpense != null ? onlineShoppingExpense : 0,
                offlineShoppingExpense != null ? offlineShoppingExpense : 0,
                medicalExpense != null ? medicalExpense : 0,
                communicationExpense != null ? communicationExpense : 0,
                insuranceExpense != null ? insuranceExpense : 0,
                propertyExpense != null ? propertyExpense : 0,
                travelExpense != null ? travelExpense : 0,
                personalTransferExpense != null ? personalTransferExpense : 0,
                otherExpense != null ? otherExpense : 0
        );
    }

    @Override
    public String toString() {
        return "DwsSummaryRecord{" +
                "periodType='" + periodType + '\'' +
                ", periodValue='" + periodValue + '\'' +
                ", txCount=" + txCount +
                ", netExpense=" + netExpense +
                ", totalIncome=" + totalIncome +
                ", totalTransfer=" + totalTransfer +
                ", food=" + foodExpense +
                ", transport=" + transportExpense +
                ", onlineShopping=" + onlineShoppingExpense +
                ", offlineShopping=" + offlineShoppingExpense +
                ", medical=" + medicalExpense +
                ", communication=" + communicationExpense +
                ", insurance=" + insuranceExpense +
                ", property=" + propertyExpense +
                ", travel=" + travelExpense +
                ", personalTransfer=" + personalTransferExpense +
                ", other=" + otherExpense +
                '}';
    }
}
