package cn.com.shinano.ShinanoMQ.base;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;


public class MessageEncoder extends MessageToByteEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out)throws Exception{
        out.writeBytes(MessageUtil.messageTurnBytes(msg));
    }
}
