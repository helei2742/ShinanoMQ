package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.constans.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.store.AppendMessageResult;
import cn.com.shinano.ShinanoMQ.core.store.AppendMessageStatus;
import cn.com.shinano.ShinanoMQ.core.store.MappedFile;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.topic.BrokerTopicInfo;
import cn.com.shinano.ShinanoMQ.core.manager.*;
import cn.com.shinano.ShinanoMQ.core.support.IndexLogBuildSupport;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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

    @Autowired
    private IndexLogBuildSupport indexLogBuildSupport;

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
    public CompletableFuture<PutMessageResult> asyncPutMessage(Message message) {

        return CompletableFuture.supplyAsync(() -> {
            String topic = message.getTopic();
            String queue = message.getQueue();

            PutMessageResult putMessageResult = new PutMessageResult();
            putMessageResult.setTransactionId(message.getTransactionId());
            Long startOffset = brokerTopicInfo.queryTopicQueueOffset(topic, queue);

            MappedFile mappedFile = null;
            try {
                mappedFile = MappedFile.getMappedFile(topic, queue, startOffset);
            } catch (IOException e) {
                log.error("create mapped file got an error", e);
                return putMessageResult.setStatus(PutMessageStatus.CREATE_MAPPED_FILE_FAILED);
            }

            mappedFile.writeLock();
            try {
                AppendMessageResult result = mappedFile.append(message);

                switch (result.getStatus()) {
                    case PUT_OK:
                        //更新offset
                        offsetManager.updateTopicQueueOffset(topic, queue, result.getWroteOffset());

                        //TODO 构建indexLog
//                        indexLogBuildSupport.buildIndexLog(topic, queue, result.getWroteOffset());
                        return putMessageResult.setStatus(PutMessageStatus.APPEND_LOCAL);
                    case END_OF_FILE:
                        mappedFile.loadNextFile(result.getWroteOffset());

                        result = mappedFile.append(message);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                            return putMessageResult.setStatus(PutMessageStatus.APPEND_LOCAL);
                        }
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        return putMessageResult.setStatus(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED);
                }
            }catch (Exception e) {
                log.error("write message got unknown error", e);
                return putMessageResult.setStatus(PutMessageStatus.UNKNOWN_ERROR);
            }finally {
                mappedFile.writeUnlock();
            }

            return putMessageResult.setStatus(PutMessageStatus.UNKNOWN_ERROR);
        }, executor);
    }

    @Override
    public PutMessageStatus persistentBytes(String fileName,
                                            String topic,
                                            String queue,
                                            long startOffset,
                                            byte[] bytes) {
        String saveDir = StoreFileUtil.getTopicQueueSaveDir(topic, queue);
        File dataFile = new File(saveDir, fileName);
        try {
            RandomAccessFile accessFile = new RandomAccessFile(dataFile, "rw");
            accessFile.seek(startOffset);
            accessFile.write(bytes);
        } catch (IOException e) {
            log.error("persistent [{}], topic [{}], queue [{}], startOffset [{}] error",
                    fileName, topic, queue, saveDir);
            return PutMessageStatus.UNKNOWN_ERROR;
        }
        return PutMessageStatus.APPEND_LOCAL;
    }

    @Override
    public PutMessageResult syncPutMessage(Message message) {
        return waiteForFutureResult(asyncPutMessage(message), message.getTransactionId(), 0);
    }

    private PutMessageResult waiteForFutureResult(CompletableFuture<PutMessageResult> future, String tsId, int count)  {
        try {
            return future.get(BrokerConfig.LOCAL_PERSISTENT_WAIT_TIME_LIMIT, BrokerConfig.LOCAL_PERSISTENT_WAIT_TIME_UNIT);
        } catch (TimeoutException e) {
            if(count > BrokerConfig.LOCAL_PERSISTENT_WAIT_TIME_OUT_RETRY) return new PutMessageResult(tsId, PutMessageStatus.FLUSH_DISK_TIMEOUT);
            log.warn("local persistent time out, tsId[{}]", tsId);
            return waiteForFutureResult(future, tsId, count+1);
        } catch (InterruptedException | ExecutionException e) {
            return new PutMessageResult(tsId, PutMessageStatus.UNKNOWN_ERROR);
        }
    }

    @Override
    public boolean updateConsumeTimes(String topic, String queue, Long offset, Integer length, Integer retryTimes) {
        try {
            File file = StoreFileUtil.getDataFileOfLogicOffset(topic, queue, offset);
            RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
            accessFile.seek(offset);
            byte[] len = new byte[8];
            accessFile.read(len);

            int trueLength = ByteBuffer.wrap(len).getInt();
            if (trueLength + 8 != length) {
                return false;
            }
            byte[] bytes = new byte[trueLength];
            accessFile.read(bytes);

            SaveMessage message = ProtostuffUtils.deserialize(bytes, SaveMessage.class);
            message.setReconsumeTimes(retryTimes);
            accessFile.seek(offset+8);
            accessFile.write(ProtostuffUtils.serialize(message));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
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

                    byte[] bytes = BrokerUtil.messageTurnBrokerSaveBytes(msg.getMessage(), offset);

                    //追加写入
//                    this.offset = mappedFile.append(bytes);

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
