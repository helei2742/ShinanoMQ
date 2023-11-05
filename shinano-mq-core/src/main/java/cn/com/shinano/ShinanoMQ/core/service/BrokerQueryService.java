package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.base.Message;
import io.netty.channel.Channel;



public interface BrokerQueryService {
    /**
     * 查询topic下queue中消息当前的offset
     * @param message 请求体
     * @param channel 数据返回的channel
     */
    void queryTopicQueueOffset(Message message, Channel channel);

    /**
     * 查询topic下queue中 offset 位置的消息
     * @param message 请求体
     * @param channel 数据返回的channel
     */
    void queryTopicQueueOffsetMsg(Message message, Channel channel);
}
