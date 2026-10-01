package com.finance.etl.transform.extractor;

import com.finance.etl.model.RawEmail;
import java.util.Map;

/**
 * 3. 卡槽与接收端识别器 (SimSlotExtractor)
 * 职责：依据 SmsForwarder 格式特征 (SubId：1, SubId：2, SIM1, SIM2, 卡槽2 等) 识别接收短信的本机卡槽标识。
 */
public class SimSlotExtractor implements SmsFieldExtractor {
    private static final long serialVersionUID = 1L;

    @Override
    public Map<String, Object> extract(RawEmail email) {
        if (email == null) {
            return Map.of("receiverPhone", "SIM_SLOT_1");
        }

        String subject = email.getSubject() != null ? email.getSubject() : "";
        String body = email.getBody() != null ? email.getBody() : "";
        String fullContent = subject + " " + body;

        String receiverPhone = "SIM_SLOT_1";
        if (fullContent.contains("SubId：2") || fullContent.contains("SIM2") || fullContent.contains("卡槽2")) {
            receiverPhone = "SIM_SLOT_2";
        }

        return Map.of("receiverPhone", receiverPhone);
    }
}
