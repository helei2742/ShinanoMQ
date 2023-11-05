package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.core.service.BrokerQueryService;
import cn.com.shinano.ShinanoMQ.core.service.BrokerSender;
import cn.com.shinano.ShinanoMQ.core.service.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.service.TopicQueryService;
import cn.hutool.core.lang.Pair;
import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Slf4j
@Service
public class BrokerQueryServiceImpl implements BrokerQueryService {
    @Autowired
    private OffsetManager offsetManager;

    @Autowired
    private BrokerSender brokerSender;

    @Autowired
    private TopicQueryService topicQueryService;

    @Override
    public void queryTopicQueueOffset(Message message, Channel channel) {
        long l = offsetManager.queryTopicQueueOffset(message.getTopic(), message.getQueue());
        message.setValue(String.valueOf(l));
        message.setOpt(MessageOPT.BROKER_INFO_QUERY_RESULT);
        brokerSender.sendQueryBrokerResult(message, channel);
    }

    @Override
    public void queryTopicQueueOffsetMsg(Message message, Channel channel) {

        try {
            Pair<List<Message>, Long> listLongPair = topicQueryService.queryTopicQueueAfterOffsetMsg(
                    message.getTopic(),
                    message.getQueue(),
                    Long.valueOf(message.getValue()));
            if(listLongPair == null)
                message.setOpt(MessageOPT.TOPIC_QUEUE_OFFSET_MESSAGE_QUERY_404);
            else
                message.setOpt(MessageOPT.TOPIC_QUEUE_OFFSET_MESSAGE_QUERY_RESULT);
            message.setValue(JSON.toJSONString(listLongPair));
        } catch (IOException e) {
            log.error("query message[{}] get error", message, e);
        }

        brokerSender.sendQueryBrokerResult(message, channel);
    }
}
