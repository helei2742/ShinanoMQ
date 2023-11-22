package cn.com.shinano.ShinanoMQ.consmer.dto;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author lhe.shinano
 * @date 2023/11/21
 */
@EqualsAndHashCode(callSuper = true)
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
                saveMessage.getBody(),
                saveMessage.getProps());
        this.nextOffset = nextOffset;
    }

    @Override
    public String toString() {
        return "ConsumeMessage{" +
                "nextOffset=" + nextOffset +
                super.toString() +
                '}';
    }
}
