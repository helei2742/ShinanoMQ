package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import cn.com.shinano.ShinanoMQ.core.manager.TopicQueryManager;
import cn.hutool.core.util.StrUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 处理查询topic相关的信息
 */
@Slf4j
public class TopicQueryProcessor implements RequestProcessor {

    public TopicQueryProcessor(TopicQueryManager topicQueryManager) {
        this.topicQueryManager = topicQueryManager;
    }

    private final TopicQueryManager topicQueryManager;

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand, Channel channel) {
        String topicQueryOpt = remotingCommand.getExtFieldsValue(ExtFieldsConstants.TOPIC_QUERY_OPT_KEY);
        String topic = remotingCommand.getTopic();
        String queue = remotingCommand.getQueue();
        String transactionId = remotingCommand.getTransactionId();


        if(StrUtil.isBlank(topicQueryOpt)) return;
        CompletableFuture<RemotingCommand> future = null;
        switch (topicQueryOpt) {
            case TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET://查询offset
                future = topicQueryManager.queryTopicQueueOffset(topic, queue);

                break;
            case TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE://根据offset查消息
                Long offset = remotingCommand.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);
                Integer count = remotingCommand.getExtFieldsInt(ExtFieldsConstants.QUERY_TOPIC_MESSAGE_COUNT_KEY);

                if(count == null || count == 0) count = TopicQueryConstants.QUERY_TOPIC_MESSAGE_COUNT_LIMIT;
                else count = Math.min(count, TopicQueryConstants.QUERY_TOPIC_MESSAGE_COUNT_LIMIT);

                future = topicQueryManager.queryTopicQueueOffsetMsg(topic, queue, offset, count);
                break;
        }

        RemotingCommand response = null;
        try {
            if(future != null){
                response = future.get();
                response.setTransactionId(transactionId);
                response.setCode(RemotingCommandCodeConstants.SUCCESS);
                NettyChannelSendSupporter.sendMessage(response, channel);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("query topic queue offset got an error", e);
            response = RemotingCommandPool.getObject();
            response.setTransactionId(transactionId);
            response.setFlag(RemotingCommandFlagConstants.TOPIC_INFO_QUERY_RESULT);
            response.setCode(RemotingCommandCodeConstants.FAIL);
            NettyChannelSendSupporter.sendMessage(response, channel);
        }
    }
}
