package cn.com.shinano.ShinanoMQ.consmer.dto;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


/**
 * @author lhe.shinano
 * @date 2023/11/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LocalCommitLog {
    private String topic;
    private String queue;
    private Long consumeOffset;
    private byte[] batchOffset;

    public LocalCommitLog(RemotingCommand command) {
        this.topic = command.getTopic();
        this.queue = command.getQueue();
        this.consumeOffset = command.getExtFieldsLong(ExtFieldsConstants.CONSUMER_MIN_ACK_OFFSET_KEY);
        this.batchOffset = command.getBody();
    }
}

