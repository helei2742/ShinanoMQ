package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.base.Message;
import io.netty.channel.Channel;

import java.nio.charset.StandardCharsets;

public abstract class AbstractBrokerService {

    /**
     * 向channel中发送查询到的broker信息
     * @param msg 响应消息
     * @param channel  响应消息的channel
     */
    protected void sendMessage(Message msg, Channel channel) {
        channel.writeAndFlush(msg);
    }

    protected void sendMessage(Integer flag, String body, Channel channel) {
        Message message = new Message();
        message.setFlag(flag);
        message.setBody(body.getBytes(StandardCharsets.UTF_8));

        sendMessage(message, channel);
    }
}
