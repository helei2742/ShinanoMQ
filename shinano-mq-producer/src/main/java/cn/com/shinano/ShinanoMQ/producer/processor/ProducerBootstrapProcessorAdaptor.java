package cn.com.shinano.ShinanoMQ.producer.processor;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * 处理生产者收到的消息
 */
@Slf4j
public class ProducerBootstrapProcessorAdaptor extends AbstractNettyProcessorAdaptor {

    private String clientId;

    protected Channel channel;

    private ReceiveMessageProcessor receiveMessageProcessor;
    private ClientInitMsgProcessor clientInitMsgProcessor;

    public ProducerBootstrapProcessorAdaptor(String clientId) {
        this.clientId = clientId;
    }

    public void init(ClientInitMsgProcessor clientInitMsgHandler,
                     ReceiveMessageProcessor receiveMessageHandler,
                     NettyClientEventHandler eventHandler) {

        super.eventHandler = eventHandler;

        this.receiveMessageProcessor = receiveMessageHandler;
        this.clientInitMsgProcessor = clientInitMsgHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        eventHandler.activeHandler(ctx);
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
//        log.info("got message [{}]", remotingCommand);
        switch (remotingCommand.getFlag()) {
            case RemotingCommandFlagConstants.CLIENT_CONNECT_RESULT:
                if(!clientInitMsgProcessor.initClient(remotingCommand.getExtFields())) {
                    eventHandler.initSuccessHandler();
                }else {
                    eventHandler.initFailHandler();
                }
                break;
            case RemotingCommandFlagConstants.TOPIC_INFO_QUERY_RESULT:
            case RemotingCommandFlagConstants.PRODUCER_MESSAGE_RESULT:
                receiveMessageProcessor.invokeCallBack(remotingCommand.getTransactionId(), remotingCommand);
                break;
            case RemotingCommandFlagConstants.BROKER_MESSAGE_BATCH_ACK:
                receiveMessageProcessor.resolveBatchACK(remotingCommand);
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
    public void sendMsg(ChannelHandlerContext context,RemotingCommand remotingCommand) {

        sendMsg(remotingCommand);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        eventHandler.exceptionHandler(ctx, cause);
    }

    public void sendMsg(RemotingCommand remotingCommand) {
        log.info("send msg [{}]", remotingCommand);
        channel.writeAndFlush(remotingCommand);
    }
}
