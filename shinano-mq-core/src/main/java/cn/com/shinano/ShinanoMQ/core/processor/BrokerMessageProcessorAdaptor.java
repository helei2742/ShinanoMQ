package cn.com.shinano.ShinanoMQ.core.processor;

import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.core.manager.ConnectManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 处理接受到到消息，为其生产唯一到id后调用，DispatchMessageService处理
 */
@Slf4j
@Component
@ChannelHandler.Sharable
public class BrokerMessageProcessorAdaptor extends AbstractNettyProcessorAdaptor {

    private AtomicInteger connectNum;

    @Autowired
    @Qualifier("messageHandlerMap")
    private Map<Integer, RequestProcessor> messageHandlerMap;

    @Autowired
    private ConnectManager connectManager;

    public BrokerMessageProcessorAdaptor() {
        super(new NettyClientEventHandler() {
            @Override
            public void exceptionHandler(ChannelHandlerContext ctx, Throwable cause) {
                log.error("broker got an exception, ", cause);
            }
        });
        this.connectNum = new AtomicInteger(0);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        log.info("client on line, channel id：[{}], current connect count [{}]", ctx.channel().id().asLongText(), connectNum.incrementAndGet());
    }

    /**
     * 得到message后处理
     * @param ctx
     * @param remotingCommand
     */
    @Override
    protected void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        Channel channel = ctx.channel();
        log.info("get an message [{}]", remotingCommand);
        //根据消息的类型从map中取出对应的handler处理
        RequestProcessor requestProcessor = messageHandlerMap.get(remotingCommand.getFlag());
        if(requestProcessor != null) {
            requestProcessor.handlerMessage(ctx, remotingCommand, channel);
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
        log.info("client:{}, [{}] off line, current connect count [{}]",
                ctx.channel().id().asLongText(),
                ctx.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY),
                connectNum.decrementAndGet());

        // 获取channel中id
        String id = ctx.channel().attr(ShinanoMQConstants.ATTRIBUTE_KEY).get();
        // map移除channel
        connectManager.remove(id);
    }


    @Override
    public void printLog(String logStr) {
        log.info(logStr);
    }
}
