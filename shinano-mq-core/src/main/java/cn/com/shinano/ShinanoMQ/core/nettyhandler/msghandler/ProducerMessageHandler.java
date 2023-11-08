package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.MessageHandler;
import cn.com.shinano.ShinanoMQ.core.service.BrokerAckService;
import cn.com.shinano.ShinanoMQ.core.service.DispatchMessageService;
import cn.com.shinano.ShinanoMQ.core.service.TopicManager;
import cn.com.shinano.ShinanoMQ.core.service.impl.BrokerAckServiceImpl;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;


/**
 * 处理生产者发送的需要保存的数据消息
 */
@Slf4j
public class ProducerMessageHandler implements MessageHandler {

    private final TopicManager topicManager;

    private final DispatchMessageService dispatchMessageService;

    private final BrokerAckService brokerAckService;

    public ProducerMessageHandler(TopicManager topicManager, DispatchMessageService dispatchMessageService, BrokerAckService brokerAckService) {
        this.topicManager = topicManager;
        this.dispatchMessageService = dispatchMessageService;
        this.brokerAckService = brokerAckService;
    }

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel) {
        log.debug("服务端收到消息\n {}", message);

        if (!topicManager.isTopicExist(message.getTopic(), message.getQueue())) {
            brokerAckService.sendProducerCommitAck(message.getTransactionId(),
                    BrokerAckServiceImpl.AckStatus.FAIL.getValue(),
                    channel);
            return;
        }

        //生成一个唯一的messageId
        String messageId = BrokerUtil.getTransactionId(message.getTransactionId());
        message.setTransactionId(messageId);
        BrokerMessage brokerMessage = new BrokerMessage(messageId, message);

        //设置该消息的响应ACK状态
        brokerAckService.setAckFlag(messageId, channel);

        //交给下游处理
        dispatchMessageService.addMessageIntoQueue(brokerMessage);
    }
}
