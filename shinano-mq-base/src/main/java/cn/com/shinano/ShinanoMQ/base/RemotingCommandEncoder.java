package cn.com.shinano.ShinanoMQ.base;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.protocol.Serializer;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;


public class RemotingCommandEncoder extends MessageToByteEncoder<RemotingCommand> {

   /* @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out)throws Exception{
        byte[] bytes = MessageUtil.messageTurnBytes(msg);
        if(bytes.length > ShinanoMQConstants.MAX_FRAME_LENGTH) {
            throw new IllegalArgumentException("消息长度超过限制!");
        }
        byte[] length = ByteBuffer.allocate(ShinanoMQConstants.MESSAGE_SIZE_LENGTH).putInt(bytes.length).array();
        ByteBuf byteBuf = Unpooled.buffer().writeBytes(length).writeBytes(bytes);
        out.writeBytes(byteBuf);
    }*/

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out)throws Exception{
        out.writeBytes(Serializer.Algorithm.Protostuff.serialize(remotingCommand));
        remotingCommand.release();
    }
}
