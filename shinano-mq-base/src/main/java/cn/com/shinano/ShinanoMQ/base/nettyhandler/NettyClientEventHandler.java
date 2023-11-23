package cn.com.shinano.ShinanoMQ.base.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public interface NettyClientEventHandler {

    default void activeHandler(ChannelHandlerContext ctx){}

    default void closeHandler(Channel channel){}

    default void initSuccessHandler(RemotingCommand remotingCommand){}

    default void initFailHandler(){}

    void exceptionHandler(ChannelHandlerContext ctx, Throwable cause);
}
