package cn.com.shinano.ShinanoMQ.core.service;


import io.netty.channel.Channel;

public interface ConnectManager {
    /**
     * 添加netty客户端
     * @param serviceName
     * @param channel
     * @return
     */
    boolean add(String serviceName, Channel channel);

    /**
     * 移除链接的netty客户端
     * @param serviceName
     * @return
     */
    boolean remove(String serviceName);
}
