package cn.com.shinano.ShinanoMQ.core.manager.offset;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.core.manager.ConsumeOffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import cn.com.shinano.ShinanoMQ.core.manager.client.BrokerConsumerInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class LocalConsumeOffsetManager implements ConsumeOffsetManager {
    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    @Autowired
    private BrokerConsumerInfo brokerConsumerInfo;

    @Autowired
    private TopicManager topicManager;


    @Override
    public CompletableFuture<RemotingCommand> resolveConsumeOffset(String clientId, String topic, String queue, Long minACK, List<Long> offsets) {
        return CompletableFuture.supplyAsync(()->{
            RemotingCommand command = RemotingCommandPool.getObject();
            command.setFlag(RemotingCommandFlagConstants.CONSUMER_MESSAGE_RESULT);

            if(offsets == null || offsets.size() == 0 || !(minACK>=0)
                    || !topicManager.isTopicExist(topic, queue)
                    || !brokerConsumerInfo.updateConsumeOffset(clientId, topic, queue, minACK)//TODO 暂时只用最小的更新
            ){
                command.setCode(RemotingCommandCodeConstants.FAIL);
                command.addExtField(ExtFieldsConstants.CONSUMER_BATCH_ACK_RESULT, "false");
                log.warn("update consume offset fail, clientId[{}], topic[{}], queue[{}], minACK[{}]",
                        clientId, topic, queue, minACK);
                return command;
            }

            command.setCode(RemotingCommandCodeConstants.SUCCESS);
            command.addExtField(ExtFieldsConstants.CONSUMER_BATCH_ACK_RESULT, "true");
            return command;
        }, executor);
    }

}
