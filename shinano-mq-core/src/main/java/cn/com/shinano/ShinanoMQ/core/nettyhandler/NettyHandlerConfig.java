package cn.com.shinano.ShinanoMQ.core.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.SystemConstants;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.BrokerInfoQueryHandler;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.ClientConnectHandler;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.ProducerRequestHandler;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler.TopicQueryHandler;
import cn.com.shinano.ShinanoMQ.core.manager.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class NettyHandlerConfig {

    @Autowired
    private BrokerQueryManager brokerQueryManager;

    @Autowired
    private ConnectManager connectManager;

    @Autowired
    private TopicQueryManager topicQueryManager;

    @Autowired
    private TopicManager topicManager;

    @Autowired
    private DispatchMessageService dispatchMessageService;

    @Autowired
    private BrokerAckManager brokerAckManager;

    @Bean("messageHandlerMap")
    public Map<Integer, RequestHandler> messageHandlerMap() {
        Map<Integer, RequestHandler> res = new HashMap<>();

        res.put(SystemConstants.BROKER_INFO_QUERY, new BrokerInfoQueryHandler(brokerQueryManager));
        res.put(SystemConstants.CLIENT_CONNECT, new ClientConnectHandler(connectManager));
        res.put(SystemConstants.TOPIC_INFO_QUERY, new TopicQueryHandler(topicQueryManager));
        res.put(SystemConstants.PRODUCER_MESSAGE, new ProducerRequestHandler(topicManager, dispatchMessageService, brokerAckManager));
        return res;
    }
}
