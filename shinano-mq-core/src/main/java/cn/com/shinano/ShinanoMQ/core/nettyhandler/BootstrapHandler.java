package cn.com.shinano.ShinanoMQ.core.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.MessageUtil;
import cn.com.shinano.ShinanoMQ.base.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyHeartbeatHandler;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.service.BrokerAckService;
import cn.com.shinano.ShinanoMQ.core.service.BrokerQueryService;
import cn.com.shinano.ShinanoMQ.core.service.DispatchMessageService;
import cn.com.shinano.ShinanoMQ.core.service.ConnectManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;



/**
 * 处理接受到到消息，为其生产唯一到id后调用，DispatchMessageService处理
 */
@Slf4j
@Component
@ChannelHandler.Sharable
public class BootstrapHandler extends NettyHeartbeatHandler {

    @Autowired
    private DispatchMessageService dispatchMessageService;

    @Autowired
    private ConnectManager connectManager;

    @Autowired
    private BrokerAckService brokerAckService;

    @Autowired
    private BrokerQueryService brokerQueryService;

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

        if(message.getOpt().equals(MessageOPT.PRODUCER_CONNECT)) { //生产者链接
            log.info("client {} connect", message);
            String clientId = MessageUtil.getClientId(message);

            connectManager.add(clientId, channel);
        }else if(message.getOpt().equals(MessageOPT.PRODUCER_MESSAGE)) { //生产者发的消息
            log.debug("服务端收到消息\n {}", message);
            //生成一个唯一的messageId
            long messageId = BrokerUtil.getBrokerMessageId();
            BrokerMessage brokerMessage = new BrokerMessage(messageId, message);

            //设置该消息的响应ACK状态
            brokerAckService.setAckFlag(messageId, channel);

            //交给下游处理
            dispatchMessageService.addMessageIntoQueue(brokerMessage);
        }else if(message.getOpt().equals(MessageOPT.BROKER_INFO_QUERY)) { //查询broker的状态信息
            brokerQueryService.queryTopicQueueOffset(message, channel);
        } else if(message.getOpt().equals(MessageOPT.TOPIC_QUEUE_OFFSET_MESSAGE_QUERY)) {//查询topic queue 中的消息
            brokerQueryService.queryTopicQueueOffsetMsg(message, channel);
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
