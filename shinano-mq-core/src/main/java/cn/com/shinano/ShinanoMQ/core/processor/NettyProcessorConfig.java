package cn.com.shinano.ShinanoMQ.core.processor;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.core.manager.cluster.MessageInstanceSyncSupport;
import cn.com.shinano.ShinanoMQ.core.manager.topic.RetryTopicQueueManager;
import cn.com.shinano.ShinanoMQ.core.processor.msgprocessor.*;
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

    @Autowired
    private ConsumeOffsetManager consumeOffsetManager;

    @Autowired
    private RetryTopicQueueManager retryTopicQueueManager;

    @Autowired
    private MessageInstanceSyncSupport messageInstanceSyncSupport;

    @Bean("messageHandlerMap")
    public Map<Integer, RequestProcessor> messageHandlerMap() {
        Map<Integer, RequestProcessor> res = new HashMap<>();

        res.put(RemotingCommandFlagConstants.BROKER_INFO_QUERY, new BrokerInfoQueryProcessor(brokerQueryManager));
        res.put(RemotingCommandFlagConstants.CLIENT_CONNECT, new ClientConnectProcessor(connectManager));
        res.put(RemotingCommandFlagConstants.TOPIC_INFO_QUERY, new TopicQueryProcessor(topicQueryManager, consumeOffsetManager));

        res.put(RemotingCommandFlagConstants.BROKER_SYNC_PULL_MESSAGE, new ClusterSyncProcessor(topicQueryManager));

        SaveMessageRequestProcessor saveMessageRequestProcessor = new SaveMessageRequestProcessor(topicManager,
                dispatchMessageService, messageInstanceSyncSupport, brokerAckManager);
        res.put(RemotingCommandFlagConstants.PRODUCER_MESSAGE, saveMessageRequestProcessor);
        res.put(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE, saveMessageRequestProcessor);

        res.put(RemotingCommandFlagConstants.CONSUMER_MESSAGE, new ConsumerRequestProcessor(consumeOffsetManager));
        res.put(RemotingCommandFlagConstants.RETRY_CONSUME_MESSAGE, new RetryConsumeMessageProcessor(retryTopicQueueManager));

        return res;
    }
}
