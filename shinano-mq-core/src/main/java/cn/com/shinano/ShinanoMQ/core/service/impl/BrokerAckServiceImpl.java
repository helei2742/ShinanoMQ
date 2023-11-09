package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.base.dto.AckStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MessageOPT;
import cn.com.shinano.ShinanoMQ.core.config.ShinanoMQConfig;
import cn.com.shinano.ShinanoMQ.core.service.AbstractBrokerService;
import cn.com.shinano.ShinanoMQ.core.service.BrokerAckService;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
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
public class BrokerAckServiceImpl extends AbstractBrokerService implements BrokerAckService {

    private final Map<String, Integer> isPersistentMap = new ConcurrentHashMap<>();
    private final Map<String, Channel> ackChannelMap = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicInteger waitAckToProducerCount = new AtomicInteger(0);
    private final AtomicBoolean getNewAck = new AtomicBoolean(false);

    @Autowired
    private ShinanoMQConfig shinanoMQConfig;


    @Override
    public void setAckFlag(String id, Channel channel) {
        isPersistentMap.putIfAbsent(id, AckStatus.WAITE.getValue());
        ackChannelMap.putIfAbsent(id, channel);
    }


    @Override
    public void producerCommitAckSync(String id, AckStatus ack) {
        sendProducerCommitAck(id,
                ack.getValue(),
                ackChannelMap.get(id));
    }

    @Override
    public void producerCommitAckASync(String id, AckStatus ack) {
        isPersistentMap.computeIfPresent(id, (k,v)->ack.getValue());
        getNewAck.set(true);

        //超过上限，需要发送
        if(waitAckToProducerCount.incrementAndGet() >= shinanoMQConfig.getProducerCommitAckBatchSize()) {
            resolveAck();
        }
    }

    /**
     * 发送ack
     * @param id broker收到消息后为其生成的唯一id
     * @param ack   响应 ack
     * @param channel 链接的channel
     */
    @Override
    public void sendProducerCommitAck(String id, int ack, Channel channel) {
        Message message = new Message();

        message.setTransactionId(id);
        message.setFlag(MessageOPT.PRODUCER_MESSAGE_ACK);
        message.setBody(ByteBuffer.allocate(4).putInt(ack).array());

        sendMessage(message, channel);
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

            List<String> keySet = isPersistentMap.entrySet().stream()
                    .filter(e->e.getValue()!=0)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());//过滤掉等待状态的ack

            for (String tsId : keySet) {
                //发送
                sendProducerCommitAck(tsId,
                        isPersistentMap.get(tsId),
                        ackChannelMap.get(tsId));

                //移除map
                isPersistentMap.remove(tsId);
                ackChannelMap.remove(tsId);
            }
        } catch (InterruptedException e) {
            log.info("commit producer commit Ack got a error", e);
        }finally {
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

}
