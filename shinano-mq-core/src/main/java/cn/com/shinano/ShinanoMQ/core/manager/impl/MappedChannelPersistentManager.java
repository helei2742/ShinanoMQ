package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.constans.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.store.AppendMessageResult;
import cn.com.shinano.ShinanoMQ.core.store.MappedFile;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.topic.BrokerTopicInfo;
import cn.com.shinano.ShinanoMQ.core.manager.*;
import cn.com.shinano.ShinanoMQ.core.store.MappedFileManager;
import cn.com.shinano.ShinanoMQ.core.support.IndexLogBuildSupport;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;


import java.io.File;
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
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

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

    @Autowired
    private MappedFileManager mappedFileManager;

    /**
     * 持久化消息，以topic-queue 为标识创建任务加入到线程池中执行。
     * 消息从dispatchMessageService.getTopicMessageBlockingQueue(topic)的阻塞队列里获取
     *
     * @param id    由服务器生成的消息id
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


    /**
     * 异步保存消息，
     * @param message message
     * @return CompletableFuture<PutMessageResult>
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            return doPutMessage(message.getTopic(), message.getQueue(), message.getTransactionId(), true, null, null, message);
        }, executor);
    }

    /**
     * 写入消息，该方法有两种写入方式：
     * 1。传入appendOffset以及body时，则会直接将body内容插入到appendOffset后面. 如果body太长导致一个MappedFile剩余可写入空间不足
     *    则会抛出new IllegalArgumentException("put body length to big") 异常
     *
     * 2. 传入message， appendOffset以及body传入null，则将message写入到当前topic-queue的写入offset处
     * @param topic topic
     * @param queue queue
     * @param tsId tsId
     * @param insertMagic insertMagic时碰到了文件末尾，是否添加上文件末尾的魔数，选择写入方式2传入true，1传入false
     * @param appendOffset appendOffset
     * @param body body
     * @param message message
     * @IllegalArgumentException
     * @return PutMessageResult
     */
    @Override
    public PutMessageResult doPutMessage(String topic, String queue, String tsId, boolean insertMagic, Long appendOffset, byte[] body, Message message)
            throws IllegalArgumentException {
        byte[] srcBody = body;

        PutMessageResult putMessageResult = new PutMessageResult();
        putMessageResult.setTransactionId(tsId);

        MappedFile mappedFile = null;
        try {
//            mappedFile = MappedFile.getMappedFile(topic, queue, startOffset);
            Long startOffset = appendOffset == null ? brokerTopicInfo.queryTopicQueueOffset(topic, queue) : appendOffset;
            mappedFile = mappedFileManager.getMappedFile(topic, queue, startOffset);
        } catch (Exception e) {
            log.error("create mapped file got an error", e);
            return putMessageResult.setStatus(PutMessageStatus.CREATE_MAPPED_FILE_FAILED);
        }

        mappedFile.writeLock();
        try {
            if(!mappedFile.appendAble()) return doPutMessage(topic, queue, tsId, true, null, null, message);

            if (appendOffset == null) {
                appendOffset = mappedFile.getWritePos();
            }

            if (body == null && message != null) {
                body = BrokerUtil.messageTurnBrokerSaveBytes(message, appendOffset);
            }

            AppendMessageResult result = mappedFile.append(body, appendOffset, insertMagic);

            switch (result.getStatus()) {
                case PUT_OK:
                    //更新offset
                    offsetManager.updateTopicQueueOffset(topic, queue, result.getWroteOffset());
                    putMessageResult.setOffset(result.getWroteOffset());
                    putMessageResult.setContent(body);
                    putMessageResult.setMappedFile(mappedFile);
                    return putMessageResult.setStatus(PutMessageStatus.APPEND_LOCAL);
                case END_OF_FILE:
                    if (srcBody != null) {
                        throw new IllegalArgumentException("put body length to big");
                    }
                    appendOffset = result.getWroteOffset();
                    return doPutMessage(topic, queue, tsId, true, appendOffset, body, null);
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
    }


    /**
     * 直接将bytes 内容保存到对应的数据文件中
     * @param fileName fileName
     * @param topic topic
     * @param queue  queue
     * @param startOffset startOffset
     * @param bytes bytes
     * @return PutMessageStatus
     */
    @Override
    public PutMessageStatus persistentBytes(String fileName,
                                            String topic,
                                            String queue,
                                            long startOffset,
                                            byte[] bytes) {
        if (startOffset%BrokerConfig.PERSISTENT_FILE_SIZE + bytes.length > BrokerConfig.PERSISTENT_FILE_SIZE) {
            throw new IllegalArgumentException(String.format("start offset %d append bytes length %d over than file size", startOffset, bytes.length));
        }
        PutMessageResult result = doPutMessage(topic, queue, null, false, startOffset, bytes, null);
        return result.getStatus();
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
            while (!Thread.currentThread().isInterrupted()) {
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
