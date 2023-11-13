package cn.com.shinano.ShinanoMQ.core.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessor;
import cn.com.shinano.ShinanoMQ.core.manager.ConnectManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;


/**
 * 处理接受到到消息，为其生产唯一到id后调用，DispatchMessageService处理
 */
@Slf4j
@Component
@ChannelHandler.Sharable
public class BrokerMessageHandlerAdaptor extends AbstractNettyProcessor {

    @Autowired
    @Qualifier("messageHandlerMap")
    private Map<Integer, RequestHandler> messageHandlerMap;

    @Autowired
    private ConnectManager connectManager;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        log.info("client on line, channel id：[{}]", ctx.channel().id().asLongText());
    }

    /**
     * 得到message后处理
     * @param ctx
     * @param message
     */
    @Override
    protected void handlerMessage(ChannelHandlerContext ctx, Message message) {
        Channel channel = ctx.channel();
        log.info("get an message [{}]", message);
        //根据消息的类型从map中取出对应的handler处理
        RequestHandler requestHandler = messageHandlerMap.get(message.getFlag());
        if(requestHandler != null) {
            requestHandler.handlerMessage(ctx, message, channel);
        }
    }


    /**
     * channel超过设置时间没有可读消息时触发
     * @param ctx
     */
    @Override
    protected void handleReaderIdle(ChannelHandlerContext ctx) {
        super.handleReaderIdle(ctx);
        log.info("---client [{}] [{}] long time have no msg timeout, close it---",
                ctx.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY), ctx.channel().remoteAddress().toString());
        ctx.close();
    }


    /**
     * 设备下线处理
     * @param ctx
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        log.info("client:{}, [{}] off line",
                ctx.channel().id().asLongText(),
                ctx.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY));

        // 获取channel中id
        String id = ctx.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY).get();
        // map移除channel
        connectManager.remove(id);
    }

    /**
     * 设备连接异常处理
     *
     * @param ctx
     * @param cause
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 获取channel中id
        String id = ctx.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY).get();

        log.info("client:{}, request message{} got an exception", id, cause.getMessage(), cause);
    }


    @Override
    public void printLog(String logStr) {
        log.info(logStr);
    }
}
