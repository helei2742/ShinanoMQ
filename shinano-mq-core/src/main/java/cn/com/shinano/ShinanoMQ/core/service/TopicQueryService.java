package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.hutool.core.lang.Pair;

import java.io.IOException;
import java.util.List;

public interface TopicQueryService {

    /**
     * 查询topic queue 中 在offset之后的消息
     * @param topic topic name
     * @param queue queue name
     * @param offset offset
     * @return
     */
    Pair<List<Message>, Long> queryTopicQueueAfterOffsetMsg(String topic, String queue, Long offset) throws IOException;
}
