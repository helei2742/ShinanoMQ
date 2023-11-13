package cn.com.shinano.ShinanoMQ.producer.processor;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MsgFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.MsgPropertiesConstants;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessor;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.producer.processor.msgprocessor.ReceiveMessageProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

/**
 * 处理生产者收到的消息
 */
@Slf4j
public class ProducerBootstrapProcessor extends AbstractNettyProcessor {

    private String clientId;

    protected Channel channel;

    private ReceiveMessageProcessor receiveMessageProcessor;
    private ClientInitMsgProcessor clientInitMsgProcessor;

    public ProducerBootstrapProcessor(String clientId) {
        this.clientId = clientId;
    }

    public void init(Channel channel,
                     ClientInitMsgProcessor clientInitMsgHandler,
                     ReceiveMessageProcessor receiveMessageHandler,
                     NettyClientEventHandler eventHandler) {

        super.eventHandler = eventHandler;

        this.channel = channel;
        this.receiveMessageProcessor = receiveMessageHandler;
        this.clientInitMsgProcessor = clientInitMsgHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        eventHandler.activeHandler(ctx);
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, Message msg) {
        switch (msg.getFlag()) {
            case MsgFlagConstants.CLIENT_CONNECT_RESULT:
                if(!clientInitMsgProcessor.initClient(msg.getProperties())) {
                    eventHandler.initSuccessHandler();
                }else {
                    eventHandler.initFailHandler();
                }
                break;
            case MsgFlagConstants.TOPIC_INFO_QUERY_RESULT:
            case MsgFlagConstants.BROKER_MESSAGE_ACK:
                receiveMessageProcessor.invokeCallBack(msg.getTransactionId(), msg);
                break;
            case MsgFlagConstants.BROKER_MESSAGE_BATCH_ACK:
                receiveMessageProcessor.resolveBatchACK(msg);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        eventHandler.exceptionHandler(ctx, cause);
    }

    public void sendMsg(Message msg) {
        log.info("send msg [{}]", msg);
        channel.writeAndFlush(msg);
    }
}
