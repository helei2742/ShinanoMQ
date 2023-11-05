package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.core.config.ShinanoMQConfig;
import cn.com.shinano.ShinanoMQ.core.service.BrokerAckService;
import cn.com.shinano.ShinanoMQ.core.service.BrokerSender;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/**
 * 处理ACK响应
 */
@Slf4j
@Service
public class BrokerAckServiceImpl implements BrokerAckService {
    private final Map<Long, Integer> isPersistentMap = new ConcurrentHashMap<>();
    private final Map<Long, Channel> ackChannelMap = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicInteger waitAckToProducerCount = new AtomicInteger(0);
    private final AtomicBoolean getNewAck = new AtomicBoolean(false);

    @Autowired
    private ShinanoMQConfig shinanoMQConfig;

    @Autowired
    private BrokerSender brokerSender;


    @Override
    public void setAckFlag(Long id, Channel channel) {
        isPersistentMap.putIfAbsent(id, AckStatus.WAITE.getValue());
        ackChannelMap.putIfAbsent(id, channel);
    }


    @Override
    public void producerCommitAckSync(Long id, AckStatus ack) {
        brokerSender.sendProducerCommitAck(id,
                ack.getValue(),
                ackChannelMap.get(id));
    }

    @Override
    public void producerCommitAckASync(Long id, AckStatus ack) {
        isPersistentMap.computeIfPresent(id, (k,v)->ack.getValue());
        getNewAck.set(true);

        //超过上限，需要发送
        if(waitAckToProducerCount.incrementAndGet() >= shinanoMQConfig.getProducerCommitAckBatchSize()) {
            resolveAck();
        }
    }


//    @Scheduled(fixedDelay = 1000)
    public void clearAck() {
        resolveAck();
    }

    /**
     * 处理ack
     */
    public void resolveAck() {
        try {
            //没有最新的
            if(!getNewAck.compareAndSet(true, false)) return;

            boolean b = lock.tryLock(50, TimeUnit.MILLISECONDS);
            if(!b) return;

            List<Long> keySet = isPersistentMap.keySet().stream()
                    .filter(v->v!=0).collect(Collectors.toList());//过滤掉等待状态的ack

            for (Long messageId : keySet) {
                //发送
                brokerSender.sendProducerCommitAck(messageId,
                        isPersistentMap.get(messageId),
                        ackChannelMap.get(messageId));

                //移除map
                isPersistentMap.remove(messageId);
                ackChannelMap.remove(messageId);
            }
        } catch (InterruptedException e) {
            log.info("commit producer commit Ack got a error", e);
        }finally {
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    public enum AckStatus {
        SUCCESS(1),
        FAIL(-1),
        WAITE(0);

        int value;
        AckStatus(int i) {
            value = i;
        }

        public int getValue() {
            return value;
        }
    }
}
