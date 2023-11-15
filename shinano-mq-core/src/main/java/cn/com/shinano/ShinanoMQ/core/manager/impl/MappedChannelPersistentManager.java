package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.dto.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.datafile.MappedFile;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.topic.BrokerTopicInfo;
import cn.com.shinano.ShinanoMQ.core.manager.*;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 将消息持久化
 */
@Slf4j
@Component
public class MappedChannelPersistentManager extends AbstractBrokerManager implements PersistentSupport {
    /**
     * topic-key: 持久化任务
     */
    @Deprecated
    private final Map<String, PersistentTask> persistentTaskMap = new ConcurrentHashMap<>();

    /**
     * 执行持久化任务的线程池
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    @Lazy
    private DispatchMessageService dispatchMessageService;

    @Autowired
    private BrokerAckManager brokerAckManager;

    @Autowired
    @Lazy
    private OffsetManager offsetManager;

    @Autowired
    private BrokerTopicInfo brokerTopicInfo;

    /**
     * 持久化消息，以topic-queue 为标识创建任务加入到线程池中执行。
     * 消息从dispatchMessageService.getTopicMessageBlockingQueue(topic)的阻塞队列里获取
     *
     * @param id    由服务器生成的消息id
     * @param topic 消息的topic
     * @param queue 消息的queue
     */
    @Override
    public void persistentMessage(String id, String topic, String queue) {
        LinkedBlockingQueue<BrokerMessage> bq = dispatchMessageService.getTopicMessageBlockingQueue(topic);

        String persistentTaskMapKey = BrokerUtil.makeTopicQueueKey(topic, queue);

        //当前没有该topic 下 queue 的持久化任务
        persistentTaskMap.computeIfAbsent(persistentTaskMapKey, k -> {
            Long startOffset = brokerTopicInfo.queryTopicQueueOffset(topic, queue);
            PersistentTask task = new PersistentTask(bq, topic, queue, startOffset, brokerAckManager, offsetManager);
            executor.execute(task);
            return task;
        });
    }

    @Deprecated
    public Map<String, PersistentTask> getPersistentTask() {
        return persistentTaskMap;
    }


    @Override
    public CompletableFuture<BrokerResult> saveMessageImmediately(Message message) {

        return CompletableFuture.supplyAsync(() -> {
            String topic = message.getTopic();
            String queue = message.getQueue();


            BrokerResult brokerResult = new BrokerResult();
            brokerResult.setTransactionId(message.getTransactionId());

            try {
                Long startOffset = brokerTopicInfo.queryTopicQueueOffset(topic, queue);

                MappedFile mappedFile = null;
                try {
                   mappedFile = MappedFile.getMappedFile(topic, queue, startOffset);
                } catch (IOException e) {
                   log.error("create mapped file got an error", e);
                   return brokerResult.setStatus(PutMessageStatus.CREATE_MAPPED_FILE_FAILED);
                }

                byte[] bytes = BrokerUtil.messageTurnBrokerSaveBytes(message);

                //追加写入
                long offset = 0;
                try {
                    offset = mappedFile.append(bytes);
                } catch (IOException e) {
                    log.error("write message into file got an error, topic [{}], queue[{}]", topic, queue, e);
                    return brokerResult.setStatus(PutMessageStatus.PERSISTENT_MESSAGE_FAIL);
                }
                //更新offset
                offsetManager.updateTopicQueueOffset(topic, queue, offset);
                return brokerResult.setStatus(PutMessageStatus.PUT_OK);
            }catch (Exception e) {
                log.error("write message got unknown error", e);
                return brokerResult.setStatus(PutMessageStatus.UNKNOWN_ERROR);
            }
        }, executor);
    }

    /**
     * 持久化任务，每个持久化任务负责一个topic的一个key内的消息的持久化
     */
    @Deprecated
    public static class PersistentTask implements Runnable {
        private final LinkedBlockingQueue<BrokerMessage> bq;
        private final String topic;
        private final String queue;
        private final BrokerAckManager ackService;
        private final OffsetManager offsetManager;

        private Long offset;

        private MappedFile mappedFile;

        PersistentTask(LinkedBlockingQueue<BrokerMessage> bq,
                       String topic,
                       String queue,
                       long startOffset,
                       BrokerAckManager ackService,
                       OffsetManager offsetManager) {
            this.bq = bq;
            this.topic = topic;
            this.queue = queue;
            this.offset = startOffset;
            this.ackService = ackService;
            this.offsetManager = offsetManager;
        }

        @Override
        public void run() {
            while (true) {
                BrokerMessage msg = null;
                try {
                    msg = bq.take();

                    if (mappedFile == null) {
                        mappedFile = MappedFile.getMappedFile(topic, queue, offset);
                    }

                    byte[] bytes = BrokerUtil.messageTurnBrokerSaveBytes(msg.getMessage());

                    //追加写入
                    this.offset = mappedFile.append(bytes);

                    //更新offset
                    offsetManager.updateTopicQueueOffset(topic, queue, this.offset);
                    log.info("async write message done, {}, offset {}", msg, this.offset);

                    //发ACK
                    ackService.commitAck(msg.getId(), AckStatus.SUCCESS);
                    this.offset += bytes.length;
                } catch (InterruptedException | IOException e) {
                    log.error("persistent task get a error, ", e);
                    if (msg != null) {
                        ackService.commitAck(msg.getId(), AckStatus.FAIL);
                    }
                }
            }
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }
    }
}
