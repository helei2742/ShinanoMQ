package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SendCommandFuture;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.NameServerManager;
import cn.com.shinano.ShinanoMQ.core.manager.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.PersistentSupport;
import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Slf4j
@Component
public class MessageInstanceSyncSupport implements InitializingBean {
    private ExecutorService syncMsgToCLusterExecutor;

    private ClusterHost selfHost;

    private ConcurrentMap<String, SyncMessageTask> syncingQueueMap;

    @Autowired
    private NameServerManager nameServerManager;

    @Autowired
    private BrokerSpringConfig springConfig;

    @Autowired
    private BrokerClusterConnectorManager clusterConnectorManager;

    @Autowired
    private OffsetManager offsetManager;

    @Autowired
    private TopicManager topicManager;

    @Autowired
    private PersistentSupport persistentSupport;

    /**
     * master将消息同步到其它的slave节点
     * @param message message
     * @return CompletableFuture<PutMessageStatus>
     */
    public CompletableFuture<PutMessageStatus> syncMsgToInstance(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            List<ClusterHost> instances = nameServerManager.getSlaveList(springConfig.getServiceId());


            RemotingCommand remotingCommand = RemotingCommandPool.getObject();
            long offset = offsetManager.queryTopicQueueOffset(message.getTopic(), message.getQueue());
            remotingCommand.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));
            remotingCommand.setTransactionId(message.getTransactionId());
            remotingCommand.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE);
            remotingCommand.addExtField(ExtFieldsConstants.TOPIC_KEY, message.getTopic());
            remotingCommand.addExtField(ExtFieldsConstants.QUEUE_KEY, message.getQueue());
            message.setTopic(null);
            message.setQueue(null);
            message.setTransactionId(null);
            remotingCommand.setBody(message.getBody());


            for (ClusterHost instance : instances) {
                if(instance.equals(selfHost)) continue;

                BrokerClusterConnector connector = clusterConnectorManager.getConnector(instance);
                if (connector == null) continue;

                try {
                    if (connector.sendMsg(remotingCommand)) {
                        return PutMessageStatus.REMOTE_SAVE_SUCCESS;
                    }
                } catch (InterruptedException e) {
                    log.error("sync message to broker[{}] error", instance, e);
                }
            }
            return PutMessageStatus.REMOTE_SAVE_FAIL;
        }, syncMsgToCLusterExecutor);
    }

    /**
     * 开启任务，尝试从master拉取offset到targetOffset之间的消息.期间收到的message会暂存到ShinanoMQConstants.TEMP_TOPIC_PREFIX开头的
     * 暂存topic中。拉取完成后会将暂存的消息放回
     * @param message  message
     * @param offset offset
     * @param targetOffset targetOffset
     * @param channel channel
     */
    public void trySyncMsgFromInstance(Message message, long offset, long targetOffset, Channel channel) {
        CompletableFuture.runAsync(()->{
            ClusterHost master = nameServerManager.getMaster(springConfig.getServiceId());
            String key = BrokerUtil.makeTopicQueueKey(message.getTopic(), message.getQueue());


            //开启同步消息任务
            syncingQueueMap.computeIfAbsent(key, k->{
                SyncMessageTask task = new SyncMessageTask(k, master, offset, targetOffset);
                syncMsgToCLusterExecutor.execute(task);
                return task;
            });


            //消息暂存
            String tempTopic = ShinanoMQConstants.TEMP_TOPIC_PREFIX + message.getTopic();
            topicManager.createTopic(tempTopic, Collections.singletonList(message.getQueue()));

            Message tempMessage = new Message();
            tempMessage.setTransactionId(message.getTransactionId());
            tempMessage.setTopic(tempTopic);
            tempMessage.setQueue(message.getQueue());
            tempMessage.setBody(ProtostuffUtils.serialize(message));

            PutMessageResult putMessageResult = persistentSupport.syncPutMessage(tempMessage);

            channel.write(putMessageResult.handlePutMessageResult(false));
        }, syncMsgToCLusterExecutor);
    }

    @Override
    public void afterPropertiesSet() {
        this.syncingQueueMap = new ConcurrentHashMap<>();
        this.syncMsgToCLusterExecutor = Executors.newFixedThreadPool(2);

        this.selfHost = new ClusterHost(springConfig.getClientId(), springConfig.getAddress(), springConfig.getPort());
    }


    class SyncMessageTask implements Runnable{

        private final String key;

        private final ClusterHost target;
        private final long startOffset;
        private final long endOffset;
        private long currentOffset;

        public SyncMessageTask(String key, ClusterHost target, long startOffset, long endOffset) {
            this.key = key;
            this.target = target;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.currentOffset = startOffset;
        }

        @Override
        public void run() {
            while (currentOffset < endOffset) {
                SendCommandFuture future = syncMsgFromInstance(target, currentOffset);
                if (future != null) {
                    Object obj = null;
                    try {
                        obj = future.getResult();
                    } catch (InterruptedException e) {
                        log.error("request sync msg from [{}] error", target);
                    }
                    RemotingCommand response;

                    if (obj != null && !(response = (RemotingCommand) obj).equals(RemotingCommand.TIME_OUT_COMMAND)) {
                        Long endOffset = response.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);
                        if (endOffset == null) {
                            log.error("response of sync msg from [{}] didn't have offset field", target);
                            break;
                        }
                        if (endOffset > currentOffset) {
                            currentOffset = endOffset;
                        } else if(currentOffset == endOffset) {
                            break;
                        }
                    }
                }
            }

            log.warn("sync [{}] message from [{}] finish, start offset [{}], end offset [{}], target end offset [{}]",
                    key, target, startOffset, currentOffset, endOffset);

            syncingQueueMap.remove(key);
        }

        private SendCommandFuture syncMsgFromInstance(ClusterHost target, long offset) {
            BrokerClusterConnector connector = clusterConnectorManager.getConnector(target);
            RemotingCommand command = RemotingCommandPool.getObject();
            command.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_PULL_MESSAGE);
            command.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));
            return connector.sendMsgFuture(command);
        }
    }
}
