package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.cluster.MessageInstanceSyncSupport;
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

    private static final ExecutorService executor = ExecutorManager.dispatchMessageExecutor;

    @Autowired
    private PersistentSupport persistentSupport;

    @Autowired
    private BrokerAckManager brokerAckManager;

    @Autowired
    public BrokerSpringConfig brokerSpringConfig;

    @Autowired
    private MessageInstanceSyncSupport messageInstanceSyncSupport;

    /**
     * 添加message到对应topic的阻塞队列
     * @param message 服务器收到的消息，加上为其生成的唯一id
     */
    @Deprecated
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
     * @param message message
     * @param channel channel
     * @param isSyncMsgToCluster isSyncMsgToCluster
     */
    public RemotingCommand saveMessage(Message message, Channel channel, boolean isSyncMsgToCluster) {
        PutMessageResult result;
        if(brokerSpringConfig.getAsyncSendEnable()) {

            CompletableFuture<PutMessageResult> localFuture = persistentSupport.asyncPutMessage(message);

            if (isSyncMsgToCluster) {
                localFuture.thenAcceptAsync(putMessageResult ->{
                    // 保存到其它broker
                    if (putMessageResult.getStatus() == PutMessageStatus.APPEND_LOCAL) {

                        CompletableFuture<PutMessageStatus> future = messageInstanceSyncSupport.syncMsgToInstance(message.getTopic(),
                                message.getQueue(), message.getTransactionId(), putMessageResult.getContent(), putMessageResult.getOffset());

                        try {
                            PutMessageStatus syncToClusterResult = future.get();

                            putMessageResult.setStatus(syncToClusterResult);
                        } catch (InterruptedException | ExecutionException e) {
                            log.error("get sync message result error", e);
                        }
                    }

                    RemotingCommand response = putMessageResult.handlePutMessageResult(true);

                    NettyChannelSendSupporter.sendMessage(response, channel);
                }, executor);
            } else {
                localFuture.thenAcceptAsync((putMessageResult) -> {

                    RemotingCommand response = putMessageResult.handlePutMessageResult(false);

                    NettyChannelSendSupporter.sendMessage(response, channel);
                }, executor);
            }

            return null;
        } else {
            result = persistentSupport.syncPutMessage(message);

            return result.handlePutMessageResult(isSyncMsgToCluster);
        }
    }

    @Deprecated
    public void saveMessage(String topic, String queue, String tsId, Long offset, byte[] body, Channel channel) {
        PutMessageResult result = persistentSupport.doPutMessage(topic, queue, tsId, true, offset, body, null);
        RemotingCommand response = result.handlePutMessageResult(false);
//        NettyChannelSendSupporter.sendMessage(response, channel);
    }


    /**
     * 获取topic对应的阻塞队列
     * @param topic topic
     * @return topic对应的阻塞队列
     */
    @Deprecated
    public LinkedBlockingQueue<BrokerMessage> getTopicMessageBlockingQueue(String topic) {
        return dispatchMap.get(topic);
    }


}
