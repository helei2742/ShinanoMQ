package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.base.MessageUtil;
import cn.com.shinano.ShinanoMQ.core.datalog.MappedFile;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.service.BrokerAckService;
import cn.com.shinano.ShinanoMQ.core.service.DispatchMessageService;
import cn.com.shinano.ShinanoMQ.core.service.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.service.PersistentService;
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
public class MappedChannelPersistentService implements PersistentService {
    /**
     * topic-key: 持久化任务
     */
    private final Map<String, PersistentTask> persistentTaskMap = new ConcurrentHashMap<>();

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
     * 持久化消息，以topic-queue 为标识创建任务加入到线程池中执行
     * @param id 由服务器生成的消息id
     * @param topic 消息的topic
     * @param queue 消息的queue
     */
    @Override
    public void persistentMessage(long id, String topic, String queue) {
        LinkedBlockingQueue<BrokerMessage> bq = dispatchMessageService.getTopicMessageBlockingQueue(topic);

        String persistentTaskMapKey = BrokerUtil.makeTopicQueueKey(topic, queue);

        //当前没有该topic 下 queue 的持久化任务
        persistentTaskMap.computeIfAbsent(persistentTaskMapKey, k -> {
            PersistentTask task = new PersistentTask(bq, topic, queue, brokerAckService, offsetManager);
            executor.execute(task);
            return task;
        });
    }

    /**
     * 获取queue当前的offset，当没有这个queue时返回-1，存在则返回具体的offset。
     * 这个操作会在内存中没有topic-queue标识的offset时通过持久化的文件查询
     * 获取到的offset可能不是最新的
     * @param topic 消息的topic
     * @param queue 消息的queue
     * @return
     */
    @Override
    public Long tryQueryOffset(String topic, String queue) {
        String key = BrokerUtil.makeTopicQueueKey(topic, queue);
        PersistentTask persistentTask = persistentTaskMap.get(key);
        if(persistentTask == null) { //没有,从文件里查查
            return BrokerUtil.getOffsetFromFile(topic, queue);
        }
        return persistentTask.offset;
    }


    /**
     * 持久化任务，每个持久化任务负责一个topic的一个key内的消息的持久化
     */
    static class PersistentTask implements Runnable {
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

                    byte[] bytes = MessageUtil.messageTurnBrokerSaveBytes(msg.getMessage());

                    //追加写入
                    this.offset = mappedFile.append(bytes);
                    offsetManager.updateTopicQueueOffset(topic, queue, this.offset);
                    log.debug("async write message done, {}, offset {}", msg, this.offset);

                    //发ACK
                    ackService.producerCommitAckSync(msg.getId(), BrokerAckServiceImpl.AckStatus.SUCCESS);
                    this.offset += bytes.length;
                } catch (InterruptedException | IOException e) {
                    log.error("persistent task get a error, ", e);
                    if(msg != null) {
                        ackService.producerCommitAckSync(msg.getId(), BrokerAckServiceImpl.AckStatus.FAIL);
                    }
                }
            }
        }
    }
}
