package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.CompletableFuture;

public interface TopicQueryManager {

    /**
     * 查询topic下queue中消息当前的offset
     * @param topic
     * @param queue
     */
    CompletableFuture<RemotingCommand> queryTopicQueueOffset(String topic, String queue);

    /**
     * 查询topic下queue中 offset 位置的消息
     * @param topic
     * @param queue
     * @param count
     */
    CompletableFuture<RemotingCommand> queryTopicQueueOffsetMsg(String topic, String queue, long offset, int count);

    /**
     * 直接获取offset之后的byte
     * @param offset
     * @param transactionId
     * @param channel
     * @return
     */
    CompletableFuture<RemotingCommand> queryTopicQueueBytesAfterOffset(String topic, String queue, Long offset, String transactionId, Channel channel);
}
