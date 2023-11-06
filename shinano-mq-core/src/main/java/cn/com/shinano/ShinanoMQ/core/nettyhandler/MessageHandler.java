package cn.com.shinano.ShinanoMQ.core.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public interface MessageHandler {

    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel);
}
