package cn.com.shinano.ShinanoMQ.consmer.dto;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import lombok.Data;

/**
 * @author lhe.shinano
 * @date 2023/11/21
 */
@Data
public class ConsumeMessage extends SaveMessage {

    private Long nextOffset;

    public ConsumeMessage() {
    }

    public ConsumeMessage(SaveMessage saveMessage, Long nextOffset) {
        super(saveMessage.getOffset(),
                saveMessage.getLength(),
                saveMessage.getTransactionId(),
                saveMessage.getTimestamp(),
                saveMessage.getStoreHost(),
                saveMessage.getReconsumeTimes(),
                saveMessage.getBody());
        this.nextOffset = nextOffset;
    }
}
