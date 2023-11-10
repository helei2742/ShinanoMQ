package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.RequestHandler;
import cn.com.shinano.ShinanoMQ.core.manager.BrokerQueryManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * 处理查询Broker状态消息的指令消息
 */
public class BrokerInfoQueryHandler implements RequestHandler {

    public BrokerInfoQueryHandler(BrokerQueryManager brokerQueryManager) {
        this.brokerQueryManager = brokerQueryManager;
    }

    private final BrokerQueryManager brokerQueryManager;


    @Override
    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel) {

    }
}
