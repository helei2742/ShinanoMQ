package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.TopicConfig;
import cn.com.shinano.ShinanoMQ.core.manager.ConnectManager;
import cn.com.shinano.ShinanoMQ.core.manager.client.BrokerConsumerInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import io.netty.channel.Channel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理链接
 */
@Slf4j
@Service
@Component
public class LocalConnectManager implements ConnectManager {

    private final ConcurrentHashMap<String, Channel> channelMap = new ConcurrentHashMap<>();

    @Autowired
    private BrokerConsumerInfo brokerConsumerInfo;

    @Override
    public boolean remove(String clientId) {
        if(clientId == null) return false;
        return channelMap.remove(clientId) == null;
    }

    @Override
    public boolean add(String clientId, Channel channel) {
        synchronized (clientId.intern()) {
            if(channelMap.containsKey(clientId)) {
                log.warn("client [{}] has bean registry, current registry cancel", clientId);
                //重复
                channel.close();
                return false;
            }else {
                //给channel添加上标识
                channel.attr(ShinanoMQConstants.ATTRIBUTE_KEY).setIfAbsent(clientId);
                return true;
            }
        }
    }

    @Override
    public RemotingCommand buildConsumerInitCommand(String clientId) {
        RemotingCommand command = RemotingCommandPool.getObject();
        command.setFlag(RemotingCommandFlagConstants.CLIENT_CONNECT_RESULT);
        command.setCode(RemotingCommandCodeConstants.SUCCESS);
        command.setBody(ProtostuffUtils.serialize(brokerConsumerInfo.getConsumerInfo(clientId)));

        return command;
    }

    @Override
    public RemotingCommand buildProducerInitCommand(String clientId) {
        RemotingCommand command = RemotingCommandPool.getObject();

        command.setFlag(RemotingCommandFlagConstants.CLIENT_CONNECT_RESULT);
        command.addExtField(ExtFieldsConstants.SINGLE_MESSAGE_LENGTH_KEY, String.valueOf(TopicConfig.SINGLE_MESSAGE_LENGTH));
        command.addExtField(ExtFieldsConstants.QUERY_MESSAGE_MAX_COUNT_KEY, String.valueOf(TopicConfig.QUERY_MESSAGE_MAX_COUNT));

        return command;
    }
}
