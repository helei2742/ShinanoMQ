package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.MessageHandler;
import cn.com.shinano.ShinanoMQ.core.service.ConnectManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * 处理客户端链接
 */
@Slf4j
public class ClientConnectHandler implements MessageHandler {

    public ClientConnectHandler(ConnectManager connectManager) {
        this.connectManager = connectManager;
    }

    private final ConnectManager connectManager;

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel) {
        log.info("client {} connect", message);
        String clientId = MessageUtil.getClientId(message);

        connectManager.add(clientId, channel);
    }
}
