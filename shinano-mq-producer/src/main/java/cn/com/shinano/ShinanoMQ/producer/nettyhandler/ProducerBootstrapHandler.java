package cn.com.shinano.ShinanoMQ.producer.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

/**
 * 处理生产者收到的消息
 */
@Slf4j
public class ProducerBootstrapHandler extends SimpleChannelInboundHandler<Object> {
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Message message = new Message();
        message.setOpt(MessageOPT.PRODUCER_CONNECT);
        message.setValue("producer client " + new Random().nextInt(100));
        ctx.writeAndFlush(message);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object o) throws Exception {
        log.info("get ACK {}", o);
    }
}
