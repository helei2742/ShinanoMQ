package cn.com.shinano.ShinanoMQ.core.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public interface RequestHandler {

    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel);
}
