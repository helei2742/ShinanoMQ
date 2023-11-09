package cn.com.shinano.ShinanoMQ.producer.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyHeartbeatHandler;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.producer.service.ResultCallBackInvoker;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * 处理生产者收到的消息
 */
@Slf4j
public class ProducerBootstrapHandler extends NettyHeartbeatHandler {
    protected Channel channel;

    private ResultCallBackInvoker resultCallBackInvoker;

    public void init(Channel channel, ResultCallBackInvoker resultCallBackInvoker) {
        this.channel = channel;
        this.resultCallBackInvoker = resultCallBackInvoker;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Message message = new Message();
        message.setFlag(MessageOPT.CLIENT_CONNECT);
//        message.setBody((ProducerConfig.PRODUCER_CLIENT_ID.getBytes(StandardCharsets.UTF_8));
        message.setBody((ProducerConfig.PRODUCER_CLIENT_ID+ new Random().nextInt(100)).getBytes(StandardCharsets.UTF_8));

        ctx.writeAndFlush(message);
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, Message msg) {
        log.debug("client get a message [{}]", msg);
        switch (msg.getFlag()) {
            case MessageOPT.PRODUCER_MESSAGE_ACK:
            case MessageOPT.TOPIC_INFO_QUERY:
                resultCallBackInvoker.invokeCallBack(msg.getTransactionId(), msg);
                break;
        }
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
