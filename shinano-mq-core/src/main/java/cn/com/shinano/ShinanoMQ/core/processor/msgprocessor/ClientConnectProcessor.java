package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.core.config.TopicConfig;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import cn.com.shinano.ShinanoMQ.core.manager.ConnectManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;


import static cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants.CLIENT_TYPE_CONSUMER;
import static cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants.CLIENT_TYPE_PRODUCER;

/**
 * 处理客户端链接
 */
@Slf4j
public class ClientConnectProcessor implements RequestProcessor {

    public ClientConnectProcessor(ConnectManager connectManager) {
        this.connectManager = connectManager;
    }

    private final ConnectManager connectManager;

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand, Channel channel) {
        String clientId = remotingCommand.getExtFieldsValue(ExtFieldsConstants.CLIENT_ID_KEY);
        String clientType = remotingCommand.getExtFieldsValue(ExtFieldsConstants.CLIENT_TYPE_KEY);
        RemotingCommand command;

        if(connectManager.add(clientId, channel)){
            switch (clientType) {
                case CLIENT_TYPE_CONSUMER:
                    command = connectManager.buildConsumerInitCommand(clientId);
                    break;
                case CLIENT_TYPE_PRODUCER:
                    command = connectManager.buildProducerInitCommand(clientId);
                    break;
                default:
                    return;
            }
        } else {
            return;
        }

        NettyChannelSendSupporter.sendMessage(command, channel);
    }
}
