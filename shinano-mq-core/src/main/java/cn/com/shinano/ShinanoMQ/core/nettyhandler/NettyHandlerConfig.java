package cn.com.shinano.ShinanoMQ.core.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.MessageOPT;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.BrokerInfoQueryHandler;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.ClientConnectHandler;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.ProducerMessageHandler;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.TopicQueryHandler;
import cn.com.shinano.ShinanoMQ.core.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class NettyHandlerConfig {

    @Autowired
    private BrokerQueryService brokerQueryService;

    @Autowired
    private ConnectManager connectManager;

    @Autowired
    private TopicQueryService topicQueryService;

    @Autowired
    private TopicManager topicManager;

    @Autowired
    private DispatchMessageService dispatchMessageService;

    @Autowired
    private BrokerAckService brokerAckService;

    @Bean("messageHandlerMap")
    public Map<Integer, MessageHandler> messageHandlerMap() {
        Map<Integer, MessageHandler> res = new HashMap<>();

        res.put(MessageOPT.BROKER_INFO_QUERY, new BrokerInfoQueryHandler(brokerQueryService));
        res.put(MessageOPT.CLIENT_CONNECT, new ClientConnectHandler(connectManager));
        res.put(MessageOPT.TOPIC_INFO_QUERY, new TopicQueryHandler(topicQueryService));
        res.put(MessageOPT.PRODUCER_MESSAGE, new ProducerMessageHandler(topicManager, dispatchMessageService, brokerAckService));
        return res;
    }
}
