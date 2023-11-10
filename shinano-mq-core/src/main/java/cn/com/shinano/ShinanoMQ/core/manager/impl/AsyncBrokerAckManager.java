package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.VO.BatchAckVO;
import cn.com.shinano.ShinanoMQ.base.dto.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.SystemConstants;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.manager.AbstractBrokerManager;
import cn.com.shinano.ShinanoMQ.core.manager.BrokerAckManager;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;


import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;


/**
 * 异步处理ack响应
 * @author lhe.shinano
 * @date 2023/11/10
 */
@Slf4j
public class AsyncBrokerAckManager extends AbstractBrokerManager implements BrokerAckManager {
    private final ConcurrentMap<String, AckStatus> isDoneMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Channel> ackChannelMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel,ChannelAckInfo> channelCommitInfoMap = new ConcurrentHashMap<>();

    private final int commitBatchSize;
    private final long commitTTL;
    private final ExecutorService executor;

    private final Thread flushThread;

    public AsyncBrokerAckManager(int commitBatchSize, Long commitTTL, ExecutorService executor) {
        this.commitBatchSize = commitBatchSize;
        this.commitTTL = commitTTL;
        this.executor = executor;

        this.flushThread = new Thread(this::flushExpireACK, "batch-ack-cache-flush");
        this.flushThread.setDaemon(true);
        this.flushThread.start();
    }

    @Override
    public void setAckFlag(String tsId, Channel channel) {
        ackChannelMap.putIfAbsent(tsId, channel);
        isDoneMap.putIfAbsent(tsId, AckStatus.WAITE);
    }

    @Override
    public void commitAck(String tsId, AckStatus ack) {
        AckStatus ackStatus = isDoneMap.computeIfPresent(tsId, (k, v) -> {
            if (!v.equals(AckStatus.WAITE)) return null;
            return ack;
        });

        //没有这个事务id的状态，还在等待。或者没有在等待状态了，不用发ack
        if(ackStatus == null || ack.equals(AckStatus.WAITE)) {
            return;
        }

        Channel channel = ackChannelMap.get(tsId);
        channelCommitInfoMap.putIfAbsent(channel,
                new ChannelAckInfo(commitBatchSize, channel, this::sendBatchAck));

        ChannelAckInfo info = channelCommitInfoMap.get(channel);
        info.addAck(tsId, ack);

        channelCommitInfoMap.put(channel, info);
    }

    private void sendBatchAck(ChannelAckInfo ackInfo) {
        Set<String> fail = ackInfo.fail;
        Set<String> success = ackInfo.success;
        ackInfo.reset();
        Channel channel = ackInfo.channel;

        if(fail.size() == 0 && success.size() == 0) return;

        executor.execute(()->{

            BatchAckVO vo = new BatchAckVO(success, fail);
            Message message = new Message();
            message.setFlag(SystemConstants.BROKER_MESSAGE_BATCH_ACK);
            message.setBody(ProtostuffUtils.serialize(vo));

            log.info("send batch ack -- [{}]", message);
            sendMessage(message, channel);
        });
    }


    @Override
    public void sendAck(String id, int ack, Channel channel) {
        Message message = new Message();

        message.setTransactionId(id);
        message.setFlag(SystemConstants.BROKER_MESSAGE_ACK);
        message.setBody(ByteBuffer.allocate(4).putInt(ack).array());

        sendMessage(message, channel);
    }


    public void flushExpireACK() {
        while (true) {
            try {
                if (Thread.currentThread().isInterrupted()) {
                    log.warn("flush expire thread interrupted");
                    break;
                }
                log.debug("start flush Expire ACK ");
                Iterator<ChannelAckInfo> iterator = channelCommitInfoMap.values().iterator();

                if (iterator.hasNext()) {
                    ChannelAckInfo info;
                    synchronized (info = iterator.next()) {
                        if (System.currentTimeMillis() - info.lastUseTime > commitTTL) {
                            sendBatchAck(info);
                        }
                    }
                }
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (Exception e) {
                log.error("flush Expire ACK got an error", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    static class ChannelAckInfo {
        private final static AtomicIntegerFieldUpdater<ChannelAckInfo> COUNT_UPDATER;

        static {
            COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ChannelAckInfo.class, "count");
        }

        private Set<String> success;
        private Set<String> fail;
        private volatile long lastUseTime;
        private volatile int count;

        private int commitBatchSize;

        private final Channel channel;
        private final Consumer<ChannelAckInfo> callBack;

        ChannelAckInfo(int size, Channel channel, Consumer<ChannelAckInfo> fullCallBack) {
            this.commitBatchSize = size;

            this.channel = channel;

            this.callBack = fullCallBack;

            this.success = new HashSet<>();
            this.fail = new HashSet<>();
            this.lastUseTime = -1;
            this.count = 0;
        }

        public void reset() {
            lastUseTime = System.currentTimeMillis();

            COUNT_UPDATER.set(this, 0);

            lastUseTime = System.currentTimeMillis();

            this.success = new HashSet<>();
            this.fail = new HashSet<>();
        }

        public void addAck(String tsId, AckStatus ack) {


            if(ack.equals(AckStatus.SUCCESS)) success.add(tsId);
            else fail.add(tsId);

            if (COUNT_UPDATER.addAndGet(this, 1) >= commitBatchSize) {
                synchronized (this) {
                    this.callBack.accept(this);
                }
            }
        }
    }

}
