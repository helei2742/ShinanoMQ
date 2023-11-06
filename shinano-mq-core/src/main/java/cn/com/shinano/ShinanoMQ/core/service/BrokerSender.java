package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.core.service.impl.BrokerAckServiceImpl;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class BrokerSender {

    /**
     * 向channel中发送生产者提交消息的ACK
     * @param messageId 由broker收到producer发送的数据消息后为其生成的id
     * @param ack       ack
     * @param channel   响应数据的channel
     */
    public void sendProducerCommitAck(Long messageId, Integer ack, Channel channel) {
        Message message = new Message();
        message.setOpt(MessageOPT.PRODUCER_MESSAGE_ACK);
        if(ack == BrokerAckServiceImpl.AckStatus.FAIL.getValue()) {
            message.setValue("消息保存失败的ack");
        } else if(ack == BrokerAckServiceImpl.AckStatus.SUCCESS.getValue()) {
            message.setValue("发送完消息的ack");
        }

        channel.writeAndFlush(message);

        log.debug("messageId[{}] send ACK[{}]", message, ack);
    }

    /**
     * 向channel中发送查询到的broker信息
     * @param response 响应消息
     * @param channel  响应消息的channel
     */
    public void sendQueryBrokerResult(Message response, Channel channel) {
        channel.writeAndFlush(response);
        log.info("client [{}], query broker info [{}]",
                channel.attr(ShinanoMQConstants.ATTRIBUTE_KEY).get(), response);
    }
}
