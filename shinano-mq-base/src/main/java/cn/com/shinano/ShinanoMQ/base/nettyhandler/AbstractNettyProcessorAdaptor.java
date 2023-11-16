package cn.com.shinano.ShinanoMQ.base.nettyhandler;


import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;


/**
 * 处理broker与client之间的心跳
 */
public abstract class AbstractNettyProcessorAdaptor extends SimpleChannelInboundHandler<RemotingCommand> implements NettyBaseHandler {

    private int heartbeatCount = 0;

    protected ReceiveMessageProcessor receiveMessageProcessor;

    protected ClientInitMsgProcessor clientInitMsgProcessor;

    public NettyClientEventHandler eventHandler;

    public void init(ClientInitMsgProcessor clientInitMsgHandler,
                     ReceiveMessageProcessor receiveMessageHandler,
                     NettyClientEventHandler eventHandler) {

        this.eventHandler = eventHandler;
        this.receiveMessageProcessor = receiveMessageHandler;
        this.clientInitMsgProcessor = clientInitMsgHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        eventHandler.activeHandler(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, RemotingCommand remotingCommand) throws Exception {
        Integer opt = remotingCommand.getFlag();
        if(opt.equals(RemotingCommandFlagConstants.BROKER_PING)) {
            sendPongMsg(context);
        } else if (opt.equals(RemotingCommandFlagConstants.BROKER_PONG)){
            printLog(String.format("get pong msg from [%s][%s] ",
                    context.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY).get(),
                    context.channel().remoteAddress()));
        }else {
            handlerMessage(context, remotingCommand);
        }
    }

    protected abstract void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        eventHandler.exceptionHandler(ctx, cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // IdleStateHandler 所产生的 IdleStateEvent 的处理逻辑.
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            switch (e.state()) {
                case READER_IDLE:
                    handleReaderIdle(ctx);
                    break;
                case WRITER_IDLE:
                    handleWriterIdle(ctx);
                    break;
                case ALL_IDLE:
                    handleAllIdle(ctx);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 超过限定时间channel没有读时触发
     * @param ctx
     */
    protected void handleReaderIdle(ChannelHandlerContext ctx) {
    }

    /**
     * 超过限定时间channel没有写时触发
     * @param ctx
     */
    protected void handleWriterIdle(ChannelHandlerContext ctx) {
    }

    /**
     * 超过限定时间channel没有读写时触发
     * @param ctx
     */
    protected void handleAllIdle(ChannelHandlerContext ctx) {
    }

    protected void sendPingMsg(ChannelHandlerContext context) {
        RemotingCommand remotingCommand = RemotingCommandPool.getObject();
        remotingCommand.setFlag(RemotingCommandFlagConstants.BROKER_PING);
        sendMsg(context, remotingCommand);
        printLog(String.format("send ping msg to [%s], hear beat count [%d]",
                context.channel().remoteAddress(), heartbeatCount++));
    }

    protected void sendPongMsg(ChannelHandlerContext context) {
        RemotingCommand remotingCommand = RemotingCommandPool.getObject();
        remotingCommand.setFlag(RemotingCommandFlagConstants.BROKER_PONG);
        sendMsg(context, remotingCommand);
        printLog(String.format("send pong msg to [%s], hear beat count [%d]",
                context.channel().remoteAddress(), heartbeatCount++));
    }

    @Override
    public void sendMsg(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        NettyChannelSendSupporter.sendMessage(remotingCommand, context.channel());
    }
}