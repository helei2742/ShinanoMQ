package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.hutool.core.lang.Pair;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.List;

public interface TopicQueryService {

    /**
     * 查询topic下queue中消息当前的offset
     * @param message 请求体

     * @param channel 数据返回的channel
     */
    void queryTopicQueueOffset(Message message, Channel channel);

    /**
     * 查询topic下queue中 offset 位置的消息
     * @param message 请求体
     * @param count 获取数量
     * @param channel 数据返回的channel
     */
    void queryTopicQueueOffsetMsg(Message message, int count, Channel channel);

}
