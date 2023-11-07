package cn.com.shinano.ShinanoMQ.producer.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyHeartbeatHandler;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * 处理生产者收到的消息
 */
@Slf4j
public class ProducerBootstrapHandler extends NettyHeartbeatHandler {
    protected Channel channel;

    public void init(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Message message = new Message();
        message.setFlag(MessageOPT.CLIENT_CONNECT);
        message.setBody(ProducerConfig.PRODUCER_CLIENT_ID.getBytes(StandardCharsets.UTF_8));

        ctx.writeAndFlush(message);
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, Message msg) {

        log.debug("get msg [{}]", msg);

    }

    @Override
    protected void handleAllIdle(ChannelHandlerContext ctx) {
        super.handleAllIdle(ctx);
        sendPingMsg(ctx);
    }

    @Override
    public void printLog(String logStr) {
        log.info(logStr);
    }

    @Override
    public void sendMsg(ChannelHandlerContext context, Message msg) {
        sendMsg(msg);
    }

    public void sendMsg(Message msg) {
        channel.writeAndFlush(msg);
    }
}
