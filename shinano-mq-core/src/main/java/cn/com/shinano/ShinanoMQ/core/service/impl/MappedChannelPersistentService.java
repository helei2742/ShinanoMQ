package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.base.dto.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.datalog.MappedFile;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.service.*;
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
public class MappedChannelPersistentService extends AbstractBrokerService implements PersistentService {
    /**
     * topic-key: 持久化任务
     */
    private final Map<String, PersistentTask> persistentTaskMap = new ConcurrentHashMap<>();

    /**
     * topic-key: MappedFile
     */
    private final Map<String, MappedFile> mappedFileMap = new ConcurrentHashMap<>();

    /**
     * 执行持久化任务的线程池
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    @Lazy
    private DispatchMessageService dispatchMessageService;

    @Autowired
    private BrokerAckService brokerAckService;

    @Autowired
    @Lazy
    private OffsetManager offsetManager;

    /**
     * 持久化消息，以topic-queue 为标识创建任务加入到线程池中执行。
     * 消息从dispatchMessageService.getTopicMessageBlockingQueue(topic)的阻塞队列里获取
     * @param id 由服务器生成的消息id
     * @param topic 消息的topic
     * @param queue 消息的queue
     */
    @Override
    @Deprecated
    public void persistentMessage(String id, String topic, String queue) {
        LinkedBlockingQueue<BrokerMessage> bq = dispatchMessageService.getTopicMessageBlockingQueue(topic);

        String persistentTaskMapKey = BrokerUtil.makeTopicQueueKey(topic, queue);

        //当前没有该topic 下 queue 的持久化任务
        persistentTaskMap.computeIfAbsent(persistentTaskMapKey, k -> {
            PersistentTask task = new PersistentTask(bq, topic, queue, brokerAckService, offsetManager);
            executor.execute(task);
            return task;
        });
    }

    @Override
    @Deprecated
    public PersistentTask getPersistentTask(String topic, String queue) {
        String key = BrokerUtil.makeTopicQueueKey(topic, queue);
        return persistentTaskMap.get(key);
    }


    @Override
    public void saveMessageImmediately(Message message) {
        executor.execute(()->{
            String topic = message.getTopic();
            String queue = message.getQueue();

            String key = BrokerUtil.makeTopicQueueKey(topic, queue);
            try {
                mappedFileMap.putIfAbsent(key, MappedFile.getMappedFile(topic, queue));

                MappedFile mappedFile = mappedFileMap.get(key);

                byte[] bytes = BrokerUtil.messageTurnBrokerSaveBytes(message);

                //追加写入
                long offset = mappedFile.append(bytes);

                //更新offset
                offsetManager.updateTopicQueueOffset(topic, queue, offset);

                //发ACK
                brokerAckService.producerCommitAckSync(message.getTransactionId(), AckStatus.SUCCESS);
            } catch (IOException e) {
                log.error("save message got an error", e);
                brokerAckService.producerCommitAckSync(message.getTransactionId(),AckStatus.FAIL);
            }
        });
    }


    /**
     * 持久化任务，每个持久化任务负责一个topic的一个key内的消息的持久化
     */
    @Deprecated
    public static class PersistentTask implements Runnable {
        private final LinkedBlockingQueue<BrokerMessage> bq;
        private final String topic;
        private final String queue;
        private final BrokerAckService ackService;
        private final OffsetManager offsetManager;

        private Long offset;

        private MappedFile mappedFile;

        PersistentTask(LinkedBlockingQueue<BrokerMessage> bq,
                       String topic,
                       String queue,
                       BrokerAckService ackService,
                       OffsetManager offsetManager) {
            this.bq = bq;
            this.topic = topic;
            this.queue = queue;
            this.offset = 0L;
            this.ackService = ackService;
            this.offsetManager = offsetManager;
        }

        @Override
        public void run() {
            while (true) {
                BrokerMessage msg = null;
                try {
                    msg = bq.take();

                    if(mappedFile == null) {
                        mappedFile = MappedFile.getMappedFile(topic, queue);
                    }
                    byte[] bytes = BrokerUtil.messageTurnBrokerSaveBytes(msg.getMessage());

                    //追加写入
                    this.offset = mappedFile.append(bytes);

                    //更新offset
                    offsetManager.updateTopicQueueOffset(topic, queue, this.offset);
                    log.debug("async write message done, {}, offset {}", msg, this.offset);

                    //发ACK
                    ackService.producerCommitAckSync(msg.getId(), AckStatus.SUCCESS);
                    this.offset += bytes.length;
                } catch (InterruptedException | IOException e) {
                    log.error("persistent task get a error, ", e);
                    if(msg != null) {
                        ackService.producerCommitAckSync(msg.getId(),AckStatus.FAIL);
                    }
                }
            }
        }
        public Long getOffset() {
            return offset;
        }
    }
}
