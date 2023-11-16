package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.core.config.TopicConfig;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import cn.com.shinano.ShinanoMQ.core.manager.ConnectManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

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

        if(connectManager.add(clientId, channel)){ //链接成功返回服务的配置信息
            remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_CONNECT_RESULT);

            remotingCommand.addExtField(ExtFieldsConstants.SINGLE_MESSAGE_LENGTH_KEY, String.valueOf(TopicConfig.SINGLE_MESSAGE_LENGTH));
            remotingCommand.addExtField(ExtFieldsConstants.QUERY_MESSAGE_MAX_COUNT_KEY, String.valueOf(TopicConfig.QUERY_MESSAGE_MAX_COUNT));

            channel.writeAndFlush(remotingCommand);
        }
    }
}
