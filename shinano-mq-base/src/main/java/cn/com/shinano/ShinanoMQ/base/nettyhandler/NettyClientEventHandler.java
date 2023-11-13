package cn.com.shinano.ShinanoMQ.base.nettyhandler;

import io.netty.channel.ChannelHandlerContext;

public interface NettyClientEventHandler {

    default void activeHandler(ChannelHandlerContext ctx){}

    default void closeHandler(){}

    default void initSuccessHandler(){}

    default void initFailHandler(){}

    void exceptionHandler(ChannelHandlerContext ctx, Throwable cause);
}
