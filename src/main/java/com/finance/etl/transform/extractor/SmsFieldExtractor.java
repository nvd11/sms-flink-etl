package com.finance.etl.transform.extractor;

import com.finance.etl.model.RawEmail;
import java.io.Serializable;
import java.util.Map;

/**
 * 动账报文字段提取器纯函数契约接口
 * 职责：接收只读不可变的 RawEmail，返回 [字段名 -> 字段值] 的不可变 Map。
 * 支持单字段抽取（如 Map.of("sender", "95508")），
 * 也支持复合抽取器一次性返回多个强关联字段（如 Map.of("amount", 21.24, "cardLast4", "3342")）。
 */
public interface SmsFieldExtractor extends Serializable {

    /**
     * 纯函数执行：输入只读邮件，返回提取出的字段字典映射 Map
     * 
     * @param email 只读的原始邮件输入对象
     * @return 提取成功的字段字典映射 Map (绝不返回 null，若无匹配可返回 Map.of())
     */
    Map<String, Object> extract(RawEmail email);

    /**
     * 当前提取器的名称标识
     */
    default String getName() {
        return getClass().getSimpleName();
    }
}
