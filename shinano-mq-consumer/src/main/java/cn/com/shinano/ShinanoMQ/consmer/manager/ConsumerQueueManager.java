package cn.com.shinano.ShinanoMQ.consmer.manager;

import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.dto.TopicQueueData;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.dto.QueueData;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;


import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class ConsumerQueueManager {

    private final ShinanoConsumerClient consumerClient;
    private ConcurrentMap<String, ConcurrentMap<String, QueueData>> consumerQueue;

    private final ExecutorService messageHandlerExecutor = Executors.newFixedThreadPool(2);
    private final ExecutorService pullMessageExecutor = Executors.newFixedThreadPool(2);

    public ConsumerQueueManager(ShinanoConsumerClient shinanoConsumerClient) {
        this.consumerClient = shinanoConsumerClient;
    }

    /**
     * 添加消息到待消费的队列
     * @param topic
     * @param queue
     * @param vo
     */
    public void appendMessages(String topic, String queue, MessageListVO vo) {
        if(vo.getNextOffset() == -1) {
            log.debug("no more message, topic[{}], queue[{}], message[{}]", topic, queue, vo);
            return;
        }
        consumerQueue.putIfAbsent(topic, new ConcurrentHashMap<>());

        ConcurrentMap<String, QueueData> queueMap = consumerQueue.get(topic);
        queueMap.compute(queue, (k,v)->{
            if(v == null) {
                return new QueueData(queue, vo.getNextOffset(), new LinkedBlockingQueue<>(vo.getMessages()));
            }else {
                v.appendNewMessages(vo.getMessages(), vo.getNextOffset());
                return v;
            }
        });
    }


    public void onMessageReceive(String topic, String queue, Consumer<SaveMessage> handler) {
        QueueData queueData = consumerQueue.get(topic).get(queue);

        queueData.lock();
        try {
            if(queueData.canPullNew()) {
                System.out.println("--1--");
                CompletableFuture.runAsync(() -> {
                    tryPrePullMessage(topic, queue, queueData);
                }, pullMessageExecutor);
            }
        } finally {
            queueData.unlock();
        }

        CompletableFuture<Pair<SaveMessage, Integer>> future = CompletableFuture.supplyAsync(() -> {
            Pair<SaveMessage, Integer> pair = null;
            try {
                pair = queueData.getMessage();
            } catch (InterruptedException e) {
                log.error("get message error", e);
                return null;
            }
            log.debug("receive message [{}], local have [{}]", pair.getKey(), pair.getValue());
            handler.accept(pair.getKey());
            return pair;
        }, messageHandlerExecutor);

        future.thenAcceptAsync(pair->{
            if(pair == null) return;

            //本地消息不够了，拉新的
            tryPrePullMessage(topic, queue, queueData);
            //TODO 更新 broker 的 consumer offset
        }, pullMessageExecutor);
    }

    private void tryPrePullMessage(String topic, String queue, QueueData queueData) {
        if(queueData.canPullNew()) {
            log.debug("local need pull batch message, topic[{}]-queue[{}]", topic, queue);
            consumerClient.pullMessageAfterOffset(topic, queue, queueData.getNextOffset(),
                    ConsumerConfig.PRE_PULL_MESSAGE_COUNT);
        }
    }

    public void initConsumerInfo(Map<String, TopicQueueData> consumerInfo) {
        this.consumerQueue = new ConcurrentHashMap<>();
        consumerInfo.forEach((topic, tqd)->{
            ConcurrentMap<String, QueueData> value = tqd.getQueueInfoList().stream().collect(Collectors.toConcurrentMap(Pair::getKey,
                    pair -> new QueueData(pair.getKey(), pair.getValue(), new LinkedBlockingQueue<>())));
            this.consumerQueue.put(topic, value);
        });
    }
}
