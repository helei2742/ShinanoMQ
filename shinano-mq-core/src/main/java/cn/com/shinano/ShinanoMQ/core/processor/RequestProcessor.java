package cn.com.shinano.ShinanoMQ.core.processor;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public interface RequestProcessor {

    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand, Channel channel);
}
