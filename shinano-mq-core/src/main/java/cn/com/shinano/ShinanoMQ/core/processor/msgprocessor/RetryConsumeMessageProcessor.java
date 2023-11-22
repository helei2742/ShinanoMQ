package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.RetryMessage;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.core.manager.topic.RetryTopicQueueManager;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;


/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
@Slf4j
public class RetryConsumeMessageProcessor implements RequestProcessor {

    private final RetryTopicQueueManager retryTopicQueueManager;

    public RetryConsumeMessageProcessor(RetryTopicQueueManager retryTopicQueueManager) {
        this.retryTopicQueueManager = retryTopicQueueManager;
    }

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand, Channel channel) {
        String tsId = remotingCommand.getTransactionId();
        String clientId = remotingCommand.getClientId();
        String topic = remotingCommand.getTopic();
        String queue = remotingCommand.getQueue();

        RetryMessage retryMessage = new RetryMessage(remotingCommand.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY),
                remotingCommand.getExtFieldsInt(ExtFieldsConstants.SINGLE_MESSAGE_LENGTH_KEY),
                remotingCommand.getExtFieldsInt(ExtFieldsConstants.RETRY_COUNT_KEY));

        RemotingCommand response = retryTopicQueueManager.pushIntoRetryQueue(clientId, topic, queue, retryMessage);

        response.setTransactionId(tsId);
        NettyChannelSendSupporter.sendMessage(response, channel);
    }


}
