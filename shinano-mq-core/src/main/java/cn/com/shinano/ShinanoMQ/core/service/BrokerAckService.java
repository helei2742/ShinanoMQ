package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.core.service.impl.BrokerAckServiceImpl;
import io.netty.channel.Channel;

public interface BrokerAckService {

    /**
     * 初始化设置ACK状态
     * @param id message的唯一id
     */
    public void setAckFlag(String id, Channel channel);

    /**
     * 同步提交ACK,直接写channel
     * @param id message的唯一id
     * @param ack ack状态
     */
    public void producerCommitAckSync(String id, BrokerAckServiceImpl.AckStatus ack);

    /**
     * 异步提交ack，分批写channel
     * @param id message的唯一id
     * @param ack ack状态
     */
    @Deprecated
    public void producerCommitAckASync(String id, BrokerAckServiceImpl.AckStatus ack);
}
