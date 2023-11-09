package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.MessageHandler;
import cn.com.shinano.ShinanoMQ.core.service.BrokerQueryService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * 处理查询Broker状态消息的指令消息
 */
public class BrokerInfoQueryHandler implements MessageHandler {

    public BrokerInfoQueryHandler(BrokerQueryService brokerQueryService) {
        this.brokerQueryService = brokerQueryService;
    }

    private final BrokerQueryService brokerQueryService;


    @Override
    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel) {

    }
}
