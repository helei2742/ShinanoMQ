package cn.com.shinano.ShinanoMQ.core.manager.offset;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.core.dto.OffsetBufferEntity;
import cn.com.shinano.ShinanoMQ.core.manager.ConsumeOffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import cn.com.shinano.ShinanoMQ.core.manager.client.BrokerConsumerInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class LocalConsumeOffsetManager implements ConsumeOffsetManager {
    private static final String OFFSET_MAP_KEY_SEPARATOR = "!@!";

    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    private final ConcurrentMap<String, PriorityBlockingQueue<OffsetBufferEntity>> offsetBufferMap = new ConcurrentHashMap<>();
    /**
     * 01 broker端消费进度管理给每个client下topic-queue创建一个固定大小的缓冲区用来装需要收到消费offset的ack，
     * 收到ack后更新缓冲区，将头部连续的已收到的剔除，当前的消费进度就为缓冲区的第一个
     * consumer拉消息的时候添加新的offset到缓冲区， 只给缓冲区可装填大小的给consumer（待定）
     * 更新consume offset需要对缓冲区进行排序，可定时操作，不每次都更新
     *
     * 为了更加方便的获取下一个，要更新存储时的结构，自定义，不用工具转换为byte
     * OffsetBufferEntity{
     *     offset,
     *     length
     * }
     */
    @Autowired
    private BrokerConsumerInfo brokerConsumerInfo;

    @Autowired
    private TopicManager topicManager;


    @Override
    public CompletableFuture<RemotingCommand> resolveConsumeOffset(String clientId, String topic, String queue, Long minACK, List<Long> offsets) {
        return CompletableFuture.supplyAsync(()->{
            RemotingCommand command = RemotingCommandPool.getObject();
            command.setFlag(RemotingCommandFlagConstants.CONSUMER_MESSAGE_RESULT);
            //TODO offsetBufferMap与offsets处理，
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

    @Override
    public void registryWaitAckOffset(String clientId, String topic, String queue, ArrayList<SaveMessage> saveMessages) {
        String key = getKey(clientId, topic, queue);
        List<OffsetBufferEntity> list = saveMessages.stream()
                .map(s -> new OffsetBufferEntity(s.getOffset(), s.getLength()))
                .collect(Collectors.toList());
        PriorityBlockingQueue<OffsetBufferEntity> q =
                offsetBufferMap.getOrDefault(key, new PriorityBlockingQueue<OffsetBufferEntity>(1000,(e1,e2)->e1.getOffset().compareTo(e2.getOffset())));
        q.addAll(list);

        offsetBufferMap.put(key, q);
    }

    private String getKey(String clientId, String topic, String queue) {
        return clientId + OFFSET_MAP_KEY_SEPARATOR + topic + OFFSET_MAP_KEY_SEPARATOR + queue + OFFSET_MAP_KEY_SEPARATOR;
    }

    private String[] getPropFromKey(String key) {
        return key.split(OFFSET_MAP_KEY_SEPARATOR);
    }
}
