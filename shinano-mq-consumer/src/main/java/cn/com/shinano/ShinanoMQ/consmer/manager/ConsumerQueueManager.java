package cn.com.shinano.ShinanoMQ.consmer.manager;

import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Pair;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.TopicQueueData;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.listener.ConsumerOnMsgListener;
import lombok.extern.slf4j.Slf4j;


import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class ConsumerQueueManager {

    private final ShinanoConsumerClient consumerClient;

    private final ConsumerOffsetManager offsetManager;

    private final ConcurrentMap<String, ConcurrentMap<String, QueueData>> consumerQueue;
    private final ExecutorService pullMessageExecutor = Executors.newFixedThreadPool(2);


    public ConsumerQueueManager(ShinanoConsumerClient shinanoConsumerClient) {
        this.consumerQueue = new ConcurrentHashMap<>();
        this.consumerClient = shinanoConsumerClient;
        this.offsetManager = new ConsumerOffsetManager(shinanoConsumerClient);
    }

    /**
     * 添加消息到待消费的队列
     * @param topic
     * @param queue
     * @param vo
     */
    public void appendMessages(String topic, String queue, MessageListVO vo) {
        if (vo.getNextOffset() == -1) {
            log.debug("no more message, topic[{}], queue[{}], message[{}]", topic, queue, vo);
            return;
        }
        consumerQueue.putIfAbsent(topic, new ConcurrentHashMap<>());

        ConcurrentMap<String, QueueData> queueMap = consumerQueue.get(topic);

        queueMap.computeIfPresent(queue, (k, v) -> {
            v.appendNewMessages(vo.getMessages(), vo.getNextOffset());
            return v;
        });
    }

    /**
     * 添加收到消息后的监听
     * @param topic
     * @param queue
     * @param listener
     */
    public void onMessageReceive(String topic, String queue, ConsumerOnMsgListener listener) {
        if (!consumerQueue.containsKey(topic) || !consumerQueue.get(topic).containsKey(queue)) {
            log.error("don't have topic [{}], queue[{}] info", topic, queue);
            return;
        }

        QueueData queueData = consumerQueue.get(topic).get(queue);

        queueData.addListener(listener);
    }


    /**
     * 根据传入的consumerInfo初始化，对每个queue添加一个线程池任务用于拉取消息
     * @param consumerInfo
     */
    public void initConsumerInfo(Map<String, TopicQueueData> consumerInfo) {
        if(consumerInfo == null) return;
        consumerInfo.forEach((topic, tqd) -> {
            ConcurrentMap<String, QueueData> value = tqd.getQueueInfoList().stream().collect(Collectors.toConcurrentMap(Pair::getKey,
                    pair -> {
                        QueueData queueData = new QueueData(topic,
                                pair.getKey(),
                                pair.getValue(),
                                new LinkedBlockingQueue<>());

                        PullMessageTask task = new PullMessageTask(topic, queueData, consumerClient);
                        queueData.setQueueManager(this);

                        pullMessageExecutor.execute(task);
                        return queueData;
                    }));
            this.consumerQueue.put(topic, value);
        });
    }

    /**
     * 提交消费完消息的ack
     * @param transactionId transactionId
     * @param offset offset
     * @param success success
     * @param queueData queueData
     */
    public void commitConsumeACK(String transactionId, Long offset, boolean success, QueueData queueData) {
        if (success) {
            offsetManager.commitOffset(consumerClient.getClientId(),
                    queueData.getTopic(), queueData.getQueueName(), offset);
        } else {
            //TODO 失败发送到重试的queue里
            log.error("consume message fail, tsId[{}], offset[{}]", transactionId, offset);
        }
    }

    static class PullMessageTask extends Thread {
        private final String topic;
        private final QueueData queueData;
        private final ShinanoConsumerClient consumerClient;

        public PullMessageTask(String topic, QueueData queueData, ShinanoConsumerClient consumerClient) {
            this.topic = topic;
            this.queueData = queueData;
            this.consumerClient = consumerClient;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                queueData.lock();
                try {
                    while (!queueData.canPullNew()) {
                        queueData.getPullMessageCondition().await();
                    }
                    tryPrePullMessage();
                } catch (InterruptedException e) {
                    log.debug("pull message task interrupted, topic[{}]-queue[{}]", topic, queueData.getQueueName());
                } catch (Exception e) {
                    log.error("pull message task got an error, topic[{}]-queue[{}]",
                            topic, queueData.getQueueName(), e);
                    break;
                } finally {
                    queueData.unlock();
                }
            }
        }

        private void tryPrePullMessage() {
            System.out.println("offset:"+queueData.getNextOffset());
            log.debug("local need pull batch message, topic[{}]-queue[{}]", topic, queueData.getQueueName());
            consumerClient.pullMessageAfterOffset(topic, queueData.getQueueName(),
                    queueData.getNextOffset(), ConsumerConfig.PRE_PULL_MESSAGE_COUNT);
        }
    }
}
