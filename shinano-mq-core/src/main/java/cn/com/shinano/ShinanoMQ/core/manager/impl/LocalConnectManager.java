package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.core.manager.ConnectManager;
import lombok.extern.slf4j.Slf4j;
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

    @Override
    public boolean remove(String clientId) {
        if(clientId == null) return false;
        return channelMap.remove(clientId) == null;
    }

    @Override
    public boolean add(String clientId, Channel channel) {
        if (channelMap.putIfAbsent(clientId, channel) == null) { //添加成功
            //给channel添加上标识
            channel.attr(ShinanoMQConstants.ATTRIBUTE_KEY).setIfAbsent(clientId);
            return true;
        }else {
            log.warn("client [{}] has bean registry, current registry cancel", clientId);
            //重复
            channel.close();
            return false;
        }
    }
}
