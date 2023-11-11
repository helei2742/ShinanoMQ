package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.dto.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.RequestHandler;
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
public class ProducerRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    private final DispatchMessageService dispatchMessageService;

    private final BrokerAckManager brokerAckManager;

    public ProducerRequestHandler(TopicManager topicManager, DispatchMessageService dispatchMessageService, BrokerAckManager brokerAckManager) {
        this.topicManager = topicManager;
        this.dispatchMessageService = dispatchMessageService;
        this.brokerAckManager = brokerAckManager;
    }

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel) {
        log.debug("服务端收到消息\n {}", message);

        //topic不存在，返回失败
        if (!topicManager.isTopicExist(message.getTopic(), message.getQueue())) {
            brokerAckManager.sendAck(message.getTransactionId(), AckStatus.FAIL.getValue(), channel);
            return;
        }

        //生成一个唯一的messageId
        String messageId = message.getTransactionId();
//        String messageId = BrokerUtil.getTransactionId(message.getTransactionId());
//        message.setTransactionId(messageId);
        BrokerMessage brokerMessage = new BrokerMessage(messageId, message);

        //设置该消息的响应ACK状态
        brokerAckManager.setAckFlag(messageId, channel);

        //交给下游处理
        dispatchMessageService.addMessageIntoQueue(brokerMessage);
//        dispatchMessageService.saveMessageImmediately(brokerMessage);
    }
}
