package cn.com.shinano.ShinanoMQ.base;

import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {

    // 用来临时保留没有处理过的请求报文
    ByteBuf tempMsg = Unpooled.buffer();

    // in输入   --- 处理  --- out 输出
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // in 请求的数据
        // out 将粘在一起的报文拆分后的结果保留起来

        // 1、 合并报文
        ByteBuf message = null;
        int tmpMsgSize = tempMsg.readableBytes();
        // 如果暂存有上一次余下的请求报文，则合并
        if (tmpMsgSize > 0) {
            message = Unpooled.buffer();
            message.writeBytes(tempMsg);
            message.writeBytes(in);
        } else {
            message = in;
        }

        // 2、 拆分报文
        int size = message.readableBytes();
        int totalRead = 0;
        while (totalRead < size) {
            if(size <= 8) break;

            byte[] lBytes = new byte[ShinanoMQConstants.MESSAGE_SIZE_LENGTH];
            // 取出消息长度
            message.readBytes(lBytes);
            int length = ByteBuffer.wrap(lBytes).getInt();

            totalRead += lBytes.length;

            if(totalRead + length > size) { //剩下的拼不成一个消息
                break;
            }else {
                byte[] msg = new byte[length];
                message.readBytes(msg);
                totalRead += length;
                out.add(MessageUtil.bytesTurnMessage(msg));
            }
        }

        // 3、多余的报文存起来
        // 第一个报文：i+  暂存
        // 第二个报文：1 与第一次
        size = message.readableBytes();
        if (size != 0) {
            // 剩下来的数据放到tempMsg暂存
            tempMsg.clear();
            tempMsg.writeBytes(message.readBytes(size));
        }
    }
/*
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if(in.readableBytes() >= 0) {
            byte[] bytes = new byte[in.readableBytes()];
            in.readBytes(bytes);

            out.add(MessageUtil.bytesTurnMessage(bytes));
        }
    }*/
}
