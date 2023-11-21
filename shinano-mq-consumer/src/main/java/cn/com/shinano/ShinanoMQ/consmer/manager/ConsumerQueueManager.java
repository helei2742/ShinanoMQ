package cn.com.shinano.ShinanoMQ.consmer.manager;

import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.dto.Pair;
import cn.com.shinano.ShinanoMQ.base.dto.TopicQueueData;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.listener.ConsumerOnMsgListener;
import lombok.extern.slf4j.Slf4j;


import java.util.List;
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
     * @param topic topic
     * @param queue queue
     * @param vo vo 从broker拉取到的一批消息
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
     * @param topic topic
     * @param queue queue
     * @param listener listener,消息到达后，会执行相应的successHandler与failHandler方法
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
     * @param consumerInfo consumerInfo topicId:list(queue,offset)
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
            //TODO 执行失败，将消息发送到重试的queue里
            log.error("consume message fail, tsId[{}], offset[{}]", transactionId, offset);
        }
    }

    /**
     * 从broker拉取消息的任务，当QueueData里预取的消息数量不足是，会signal唤醒这个任务拉取。
     * 每个QueueData都对应一个PullMessageTask对象,在ConsumerQueueManager初始化时，被创建并
     * 放入线程池中运行，
     */
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
            log.debug("local need pull batch message, topic[{}]-queue[{}]-offset[{}]",
                    topic, queueData.getQueueName(),queueData.getNextOffset());
            consumerClient.pullMessageAfterOffset(topic, queueData.getQueueName(),
                    queueData.getNextOffset(), ConsumerConfig.PRE_PULL_MESSAGE_COUNT);
        }
    }
}
