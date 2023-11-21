package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ConsumeOffsetManager {

    /**
     * 根据提交的 ack 处理消费进度
     *
     * @param clientId
     * @param topic
     * @param queue
     * @param minOffset
     * @param offsets
     * @return
     */
    CompletableFuture<RemotingCommand> resolveConsumeOffset(String clientId, String topic, String queue, Long minOffset, List<Long> offsets);

    void registryWaitAckOffset(String clientId, String topic, String queue, ArrayList<SaveMessage> saveMessages);
}
