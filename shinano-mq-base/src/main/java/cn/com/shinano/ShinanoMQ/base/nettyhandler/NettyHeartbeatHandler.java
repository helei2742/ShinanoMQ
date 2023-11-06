package cn.com.shinano.ShinanoMQ.base.nettyhandler;


import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.ShinanoMQConstants;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;


/**
 * 处理broker与client之间的心跳
 */
public abstract class NettyHeartbeatHandler extends SimpleChannelInboundHandler<Message> implements NettyBaseHandler {
    private int heartbeatCount = 0;

    @Override
    protected void channelRead0(ChannelHandlerContext context, Message msg) throws Exception {
        Integer opt = msg.getOpt();
        if(opt.equals(MessageOPT.BROKER_PING)) {
            sendPongMsg(context);
        } else if (opt.equals(MessageOPT.BROKER_PONG)){
            printLog(String.format("client [%s] get pong msg from broker[%s], hear beat count [%s]",
                    context.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY).get(),
                    context.channel().remoteAddress(),
                    msg.getValue()));
        }else {
            handlerMessage(context, msg);
        }
    }

    protected abstract void handlerMessage(ChannelHandlerContext context, Message msg);

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

        Message message = new Message();
        message.setOpt(MessageOPT.BROKER_PONG);
        message.setValue(String.valueOf(++heartbeatCount));
        sendMsg(context, message);

        printLog(String.format("client [%s] send ping msg to broker[%s], hear beat count [%d]",
                context.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY).get(),
                context.channel().remoteAddress(), heartbeatCount));
    }

    protected void sendPongMsg(ChannelHandlerContext context) {
        Message message = new Message();
        message.setOpt(MessageOPT.BROKER_PONG);
        message.setValue(String.valueOf(++heartbeatCount));
        sendMsg(context, message);


        printLog(String.format("client [%s] send pong msg to broker[%s], hear beat count [%d]",
                context.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY).get(),
                context.channel().remoteAddress(), heartbeatCount));
    }

    @Override
    public void sendMsg(ChannelHandlerContext context, Message msg) {
        context.channel().writeAndFlush(msg);
    }
}
