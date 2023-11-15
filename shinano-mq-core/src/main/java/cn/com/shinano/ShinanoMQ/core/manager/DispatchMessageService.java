package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.AckStatus;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;


/**
 * 接受生产者的消息，分发到不同topic到消息阻塞队列
 */
@Slf4j
@Service
public class DispatchMessageService {
    private final Map<String, LinkedBlockingQueue<BrokerMessage>> dispatchMap = new ConcurrentHashMap<>();

    @Autowired
    private PersistentSupport persistentSupport;

    @Autowired
    private BrokerAckManager brokerAckManager;

    /**
     * 添加message到对应topic的阻塞队列
     * @param message 服务器收到的消息，加上为其生成的唯一id
     */

    public void addMessageIntoQueue(BrokerMessage message) {
        String topic = message.getMessage().getTopic();
        String queue = message.getMessage().getQueue();

        dispatchMap.putIfAbsent(topic, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<BrokerMessage> bq = dispatchMap.get(topic);
        bq.add(message);

        //持久化
        persistentSupport.persistentMessage(message.getId(), topic, queue);
    }

    /**
     * 直接保存
     * @param message
     * @param channel
     */
    public void saveMessageImmediately(BrokerMessage message, Channel channel) {
        //本地持久化
        CompletableFuture<BrokerResult> localFuture = persistentSupport.saveMessageImmediately(message.getMessage());

        //TODO 将来集群部署，其它broker持久化

        try {
            String tsId = message.getId();
            BrokerResult result = tryGetFutureResult(localFuture, tsId, 1);

            brokerAckManager.setAckFlag(tsId, channel);

            AckStatus ackStatus = result.getStatus().equals(PutMessageStatus.PUT_OK) ? AckStatus.SUCCESS : AckStatus.FAIL;
            brokerAckManager.commitAck(tsId, ackStatus);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private BrokerResult tryGetFutureResult(CompletableFuture<BrokerResult> future, String tsId,  int count) throws ExecutionException, InterruptedException {
        try {
            return future.get(BrokerConfig.LOCAL_PERSISTENT_WAIT_TIME_LIMIT, BrokerConfig.LOCAL_PERSISTENT_WAIT_TIME_UNIT);
        } catch (TimeoutException e) {
            if(count > BrokerConfig.LOCAL_PERSISTENT_WAIT_TIME_OUT_RETRY) return new BrokerResult(tsId, PutMessageStatus.FLUSH_DISK_TIMEOUT);
            log.warn("local persistent time out, tsId[{}]", tsId);
            return tryGetFutureResult(future, tsId, count+1);
        }
    }

    /**
     * 获取topic对应的阻塞队列
     * @param topic topic
     * @return topic对应的阻塞队列
     */
    public LinkedBlockingQueue<BrokerMessage> getTopicMessageBlockingQueue(String topic) {
        return dispatchMap.get(topic);
    }
}
