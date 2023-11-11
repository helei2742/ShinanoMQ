package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.pool.MessagePool;
import io.netty.channel.Channel;

import java.nio.charset.StandardCharsets;

public abstract class AbstractBrokerManager {

    /**
     * 向channel中发送查询到的broker信息
     * @param msg 响应消息
     * @param channel  响应消息的channel
     */
    protected void sendMessage(Message msg, Channel channel) {
        channel.writeAndFlush(msg);
    }

    /**
     * 向channel中发送查询到的broker信息
     * @param flag 消息类型
     * @param body   消息体
     * @param channel  发送消息的通道
     */
    protected void sendMessage(Integer flag, String body, Channel channel) {
        Message message = MessagePool.getObject();
//        Message message = new Message();
        message.setFlag(flag);
        message.setBody(body.getBytes(StandardCharsets.UTF_8));

        sendMessage(message, channel);
    }
}
