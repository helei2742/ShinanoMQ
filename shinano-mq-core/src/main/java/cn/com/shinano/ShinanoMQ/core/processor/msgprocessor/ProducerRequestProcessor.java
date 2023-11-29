package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.AckStatus;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.MessagePool;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import cn.com.shinano.ShinanoMQ.core.manager.BrokerAckManager;
import cn.com.shinano.ShinanoMQ.core.manager.DispatchMessageService;
import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;


/**
 * 处理生产者发送的需要保存的数据消息
 */
@Slf4j
public class ProducerRequestProcessor implements RequestProcessor {

    private final TopicManager topicManager;

    private final DispatchMessageService dispatchMessageService;

    private final BrokerAckManager brokerAckManager;

    public ProducerRequestProcessor(TopicManager topicManager, DispatchMessageService dispatchMessageService, BrokerAckManager brokerAckManager) {
        this.topicManager = topicManager;
        this.dispatchMessageService = dispatchMessageService;
        this.brokerAckManager = brokerAckManager;
    }

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand, Channel channel) {
        Message message = MessagePool.getObject();
        message.setTransactionId(remotingCommand.getTransactionId());
        message.setTopic(remotingCommand.getTopic());
        message.setQueue(remotingCommand.getQueue());
        message.setBody(remotingCommand.getBody());

        //topic不存在，返回失败
        if (!topicManager.isTopicExist(message.getTopic(), message.getQueue())) {
            brokerAckManager.sendAck(message.getTransactionId(), AckStatus.FAIL.getValue(), channel);
            return;
        }

        boolean isSyncMsgToCluster = !(remotingCommand.getFlag() == RemotingCommandFlagConstants.BROKER_ONLY_SAVE_MESSAGE);

        //设置该消息的响应ACK状态
//        brokerAckManager.setAckFlag(messageId, channel);
//        交给下游处理
//        dispatchMessageService.addMessageIntoQueue(brokerMessage);
        RemotingCommand response = dispatchMessageService.saveMessage(message, channel, isSyncMsgToCluster);
        if(response != null) {
            channel.writeAndFlush(response);
        }
    }
}
