package cn.com.shinano.ShinanoMQ.core.processor;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.core.processor.msgprocessor.BrokerInfoQueryProcessor;
import cn.com.shinano.ShinanoMQ.core.processor.msgprocessor.ClientConnectProcessor;
import cn.com.shinano.ShinanoMQ.core.processor.msgprocessor.ProducerRequestProcessor;
import cn.com.shinano.ShinanoMQ.core.processor.msgprocessor.TopicQueryProcessor;
import cn.com.shinano.ShinanoMQ.core.manager.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class NettyProcessorConfig {

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
    public Map<Integer, RequestProcessor> messageHandlerMap() {
        Map<Integer, RequestProcessor> res = new HashMap<>();

        res.put(RemotingCommandFlagConstants.BROKER_INFO_QUERY, new BrokerInfoQueryProcessor(brokerQueryManager));
        res.put(RemotingCommandFlagConstants.CLIENT_CONNECT, new ClientConnectProcessor(connectManager));
        res.put(RemotingCommandFlagConstants.TOPIC_INFO_QUERY, new TopicQueryProcessor(topicQueryManager));
        res.put(RemotingCommandFlagConstants.PRODUCER_MESSAGE, new ProducerRequestProcessor(topicManager, dispatchMessageService, brokerAckManager));
        return res;
    }
}
