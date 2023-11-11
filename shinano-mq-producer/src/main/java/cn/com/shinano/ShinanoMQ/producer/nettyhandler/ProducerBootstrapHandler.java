package cn.com.shinano.ShinanoMQ.producer.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MsgFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.MsgPropertiesConstants;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgHandler;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyHeartbeatHandler;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.producer.nettyhandler.msghandler.ReceiveMessageHandler;
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

    private String clientId;

    protected Channel channel;

    private ReceiveMessageHandler receiveMessageHandler;
    private ClientInitMsgHandler clientInitMsgHandler;

    public ProducerBootstrapHandler(String clientId) {
        this.clientId = clientId;
    }

    public void init(Channel channel,
                     ClientInitMsgHandler clientInitMsgHandler,
                     ReceiveMessageHandler receiveMessageHandler) {
        this.channel = channel;
        this.receiveMessageHandler = receiveMessageHandler;
        this.clientInitMsgHandler = clientInitMsgHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //发送一条链接消息
        Message message = new Message();
        message.setFlag(MsgFlagConstants.CLIENT_CONNECT);
        MessageUtil.setPropertiesValue(message, MsgPropertiesConstants.CLIENT_ID_KEY,
            (this.clientId+ new Random().nextInt(10000)));
//        message.setBody((ProducerConfig.PRODUCER_CLIENT_ID.getBytes(StandardCharsets.UTF_8));

        ctx.writeAndFlush(message);
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, Message msg) {
        switch (msg.getFlag()) {
            case MsgFlagConstants.CLIENT_CONNECT_RESULT:
                clientInitMsgHandler.initClient(msg.getProperties());
                break;
            case MsgFlagConstants.TOPIC_INFO_QUERY_RESULT:
            case MsgFlagConstants.BROKER_MESSAGE_ACK:
                receiveMessageHandler.invokeCallBack(msg.getTransactionId(), msg);
                break;
            case MsgFlagConstants.BROKER_MESSAGE_BATCH_ACK:
                receiveMessageHandler.resolveBatchACK(msg);
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
        log.info("send msg [{}]", msg);
        channel.writeAndFlush(msg);
    }
}
