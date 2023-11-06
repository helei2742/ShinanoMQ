package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.base.Message;
import io.netty.channel.Channel;

public abstract class AbstractBrokerService {

    /**
     * 向channel中发送查询到的broker信息
     * @param msg 响应消息
     * @param channel  响应消息的channel
     */
    protected void sendMessage(Message msg, Channel channel) {
        channel.writeAndFlush(msg);
    }
}
