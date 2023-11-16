package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.constans.AckStatus;
import io.netty.channel.Channel;

@Deprecated
public interface BrokerAckManager {

    /**
     * 初始化设置ACK状态
     * @param id message的唯一id
     */
    void setAckFlag(String id, Channel channel);

    void commitAck(String id, AckStatus ack);

    /**
     * 发送ack
     * @param id 事务id
     * @param ack   响应
     * @param channel 发送响应的channel
     */
    void sendAck(String id, int ack, Channel channel);
}
