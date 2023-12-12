package cn.com.shinano.ShinanoMQ.core.manager.offset;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.dto.OffsetBufferEntity;
import cn.com.shinano.ShinanoMQ.core.manager.ConsumeOffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.ExecutorManager;
import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import cn.com.shinano.ShinanoMQ.core.manager.client.BrokerConsumerInfo;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class LocalConsumeOffsetManager implements ConsumeOffsetManager {

    private static final ExecutorService executor = ExecutorManager.consumeOffsetExecutor;

    private final ConcurrentMap<String, PriorityQueue<OffsetBufferEntity>> offsetBufferSeqMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, HashMap<Long, OffsetBufferEntity>> offsetBufferExistMap = new ConcurrentHashMap<>();

    private final Timer updateConsumeOffsetTimer;

    @Autowired
    private BrokerConsumerInfo brokerConsumerInfo;

    @Autowired
    private TopicManager topicManager;

    public LocalConsumeOffsetManager() {
        this.updateConsumeOffsetTimer = new Timer("UpdateConsumeOffsetTimer");
        /**
         * 定时更新consume info
         */
        this.updateConsumeOffsetTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                for (String key : offsetBufferSeqMap.keySet()) {
                    PriorityQueue<OffsetBufferEntity> pq = offsetBufferSeqMap.get(key);
                    HashMap<Long, OffsetBufferEntity> map = offsetBufferExistMap.get(key);

                    long nextOffset = -1;
                    while (pq.size() > 0) {
                        OffsetBufferEntity peek = pq.peek();
                        if(peek.isSubmit()) {
                            map.remove(peek.getOffset());
                            nextOffset = peek.getOffset() + peek.getLength();
                            pq.poll();
                        }
                        else break;
                    }
                    if(nextOffset != -1) {
                        String[] prop = BrokerUtil.getPropFromKey(key);
                        brokerConsumerInfo.updateConsumeOffset(prop[0], prop[1], prop[2], nextOffset);
                        log.info("flush consume offset, {}, nextOffset [{}]", prop, nextOffset);
                    }
                }
            }
        }, BrokerConfig.BROKER_CONSUME_OFFSET_FLUSH_INTERVAL,  BrokerConfig.BROKER_CONSUME_OFFSET_FLUSH_INTERVAL);
    }


    /**
     * 更新client 的消费进度，并不是实时更新的。会更改对应offset的提交状态。updateConsumeOffsetTimer 定时的
     * 处理这些状态，将最早的已提交的offset作为最新的消费进度
     * @param clientId clientId
     * @param topic topic
     * @param queue queue
     * @param minACK minACK
     * @param offsets offsets
     * @return
     */
    @Override
    public CompletableFuture<RemotingCommand> resolveConsumeOffset(String clientId, String topic, String queue, Long minACK, List<Long> offsets) {
        return CompletableFuture.supplyAsync(()->{
            RemotingCommand command = RemotingCommandPool.getObject();
            command.setFlag(RemotingCommandFlagConstants.CONSUMER_MESSAGE_RESULT);

            //不合法
            if(offsets == null || offsets.size() == 0 || !(minACK>=0)
                    || !topicManager.isTopicExist(topic, queue)) {
                command.setCode(RemotingCommandCodeConstants.FAIL);
                command.addExtField(ExtFieldsConstants.CONSUMER_BATCH_ACK_RESULT, "false");
                log.warn("update consume offset fail, clientId[{}], topic[{}], queue[{}], minACK[{}]",
                        clientId, topic, queue, minACK);
                return command;
            }

            //更新状态
            String key = BrokerUtil.getKey(clientId, topic, queue);
            HashMap<Long, OffsetBufferEntity> map = offsetBufferExistMap.get(key);

            if (map != null) {
                for (Long offset : offsets) {
                    OffsetBufferEntity entity = map.get(offset);
                    if(entity != null) entity.setSubmit(true);
                }
            }

            command.setCode(RemotingCommandCodeConstants.SUCCESS);
            command.addExtField(ExtFieldsConstants.CONSUMER_BATCH_ACK_RESULT, "true");
            return command;
        }, executor);
    }

    /**
     * 注册client需要响应的消费成功ack
     * @param clientId clientId
     * @param topic topic
     * @param queue queue
     * @param saveMessages saveMessages
     */
    @Override
    public void registryWaitAckOffset(String clientId, String topic, String queue, List<SaveMessage> saveMessages) {
        String key = BrokerUtil.getKey(clientId, topic, queue);
        List<OffsetBufferEntity> list = saveMessages.stream()
                .map(s -> new OffsetBufferEntity(s.getOffset(), s.getLength(), false))
                .collect(Collectors.toList());

        PriorityQueue<OffsetBufferEntity> q =
                offsetBufferSeqMap.getOrDefault(key, new PriorityQueue<OffsetBufferEntity>((e1,e2)->e1.getOffset().compareTo(e2.getOffset())));

        HashMap<Long, OffsetBufferEntity> map = offsetBufferExistMap.getOrDefault(key, new HashMap<>());
        for (OffsetBufferEntity entity : list) {
            map.compute(entity.getOffset(), (k,v)->{
                if(v == null) v = entity;
                q.add(v);
                return v;
            });
        }

        offsetBufferSeqMap.put(key, q);
        offsetBufferExistMap.put(key, map);
    }

}
