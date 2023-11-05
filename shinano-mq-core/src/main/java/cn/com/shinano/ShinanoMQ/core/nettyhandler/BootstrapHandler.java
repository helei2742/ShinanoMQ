package cn.com.shinano.ShinanoMQ.core.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.MessageUtil;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.service.BrokerAckService;
import cn.com.shinano.ShinanoMQ.core.service.BrokerQueryService;
import cn.com.shinano.ShinanoMQ.core.service.DispatchMessageService;
import cn.com.shinano.ShinanoMQ.core.service.ConnectManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;



/**
 * 处理接受到到消息，为其生产唯一到id后调用，DispatchMessageService处理
 */
@Slf4j
@Component
@ChannelHandler.Sharable
public class BootstrapHandler extends SimpleChannelInboundHandler<Message> {


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
        log.info("有新的连接：[{}]", ctx.channel().id().asLongText());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message message) throws Exception {

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
            //发送
            dispatchMessageService.addMessageIntoQueue(brokerMessage);
        }else if(message.getOpt().equals(MessageOPT.BROKER_INFO_QUERY)) { //查询broker的状态信息
            brokerQueryService.queryTopicQueueOffset(message, channel);
        } else if(message.getOpt().equals(MessageOPT.TOPIC_QUEUE_OFFSET_MESSAGE_QUERY)) {//查询topic queue 中的消息
            brokerQueryService.queryTopicQueueOffsetMsg(message, channel);
        }
    }


    /**
     * 设备下线处理
     *
     * @param ctx
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        log.info("设备下线了:{}", ctx.channel().id().asLongText());
        // map中移除channel
        removeId(ctx);
    }

    /**
     * 设备连接异常处理
     *
     * @param ctx
     * @param cause
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.info("异常：{}", cause.getMessage(), cause);
        // map中移除channel
        removeId(ctx);
        // 关闭连接
        ctx.close();
    }

    private void removeId(ChannelHandlerContext ctx) {
        AttributeKey<String> key = AttributeKey.valueOf("id");
        // 获取channel中id
        String id = ctx.channel().attr(key).get();
        // map移除channel
        connectManager.remove(id);
    }
}
