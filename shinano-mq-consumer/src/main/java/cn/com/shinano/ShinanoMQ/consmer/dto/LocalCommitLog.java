package cn.com.shinano.ShinanoMQ.consmer.dto;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



/**
 * @author lhe.shinano
 * @date 2023/11/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LocalCommitLog {
    private int retryCount;
    private long lastRetryTimestamp;
    private RemotingCommand command;

    public LocalCommitLog(RemotingCommand command) {
        this.lastRetryTimestamp = -1;
        this.retryCount = 0;
        this.command = command;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }
}

