package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.constans.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;

import cn.com.shinano.ShinanoMQ.base.pool.MessagePool;
import cn.com.shinano.ShinanoMQ.core.manager.AbstractBrokerManager;
import cn.com.shinano.ShinanoMQ.core.manager.BrokerAckManager;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;


import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 同步处理ACK响应
 */
@Slf4j
@Deprecated
public class SyncBrokerAckManager extends AbstractBrokerManager implements BrokerAckManager {
    private final Map<String, Channel> ackChannelMap = new ConcurrentHashMap<>();

    @Override
    public void setAckFlag(String id, Channel channel) {
        ackChannelMap.putIfAbsent(id, channel);
    }

    @Override
    public void commitAck(String id, AckStatus ack) {
        sendAck(id,
                ack.getValue(),
                ackChannelMap.get(id));
    }

    /**
     * 发送ack
     * @param id broker收到消息后为其生成的唯一id
     * @param ack   响应 ack
     * @param channel 链接的channel
     */
    @Override
    public void sendAck(String id, int ack, Channel channel) {
        Message message = MessagePool.getObject();
//        Message message = new Message();

        message.setTransactionId(id);
        message.setFlag(RemotingCommandFlagConstants.BROKER_MESSAGE_ACK);
        message.setBody(ByteBuffer.allocate(4).putInt(ack).array());

        sendMessage(message, channel);
    }
}
