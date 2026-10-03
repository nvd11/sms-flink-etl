package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import java.io.Serializable;
import java.util.Map;

/**
 * DWD 金融动账事实字段提取器纯函数契约接口 (DwdFieldExtractor)
 * 职责：接收只读不可变的 ODS 层原始记录 SmsRecord，返回提炼出的结构化业务字段映射 Map。
 * 
 * 架构契约规范：
 * 1. 严格无副作用 (Side-effect Free) 与无外部状态依赖，各 Extractor 可独立测试、独立演进与高并发执行；
 * 2. 纯函数执行：相同的 SmsRecord 输入必须产出确定性相同的字段映射；
 * 3. 绝不返回 null：若当前报文不匹配或无提取内容，必须返回空 Map (如 Collections.emptyMap() 或 Map.of())；
 * 4. 支持单字段抽取与复合字段联动抽取 (如金额与币种、对手方与纯净商户名联动)。
 */
public interface DwdFieldExtractor extends Serializable {

    /**
     * 纯函数执行：输入只读 ODS 原始短信记录，提炼输出 DWD 事实层字段字典
     *
     * @param record ODS 层只读原始短信记录对象 (提供 id, rawBody, receivedAt, sender 等输入)
     * @return 提取成功的字段字典映射 Map (绝不返回 null，若无匹配则返回空 Map)
     */
    Map<String, Object> extract(SmsRecord record);

    /**
     * 当前提取器的名称标识 (默认返回类名，方便日志观测与链路追踪)
     */
    default String getName() {
        return getClass().isAnonymousClass() ? getClass().getName() : getClass().getSimpleName();
    }
}
