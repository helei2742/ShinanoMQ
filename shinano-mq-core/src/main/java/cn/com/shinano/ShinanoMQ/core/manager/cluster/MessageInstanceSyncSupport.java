package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.*;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.*;
import cn.com.shinano.ShinanoMQ.core.manager.topic.TopicInfo;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

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

    @Lazy
    @Autowired
    private DispatchMessageService dispatchMessageService;

    /**
     * master将消息同步到其它的slave节点
     * @param offset
     * @return CompletableFuture<PutMessageStatus>
     */
    public CompletableFuture<PutMessageStatus> syncMsgToInstance(String topic, String queue, String tsId, byte[] content, long offset) {
        return CompletableFuture.supplyAsync(() -> {
            List<ClusterHost> instances = nameServerManager.getSlaveList(springConfig.getServiceId());


            RemotingCommand remotingCommand = new RemotingCommand();
            remotingCommand.setTransactionId(tsId);
            remotingCommand.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE);
            remotingCommand.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
            remotingCommand.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
            remotingCommand.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));


            remotingCommand.setBody(content);


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


    public void saveSyncMsgInSlaveLocal(RemotingCommand request, Channel channel) {
        String tsId = request.getTransactionId();

        RemotingCommand response = new RemotingCommand();
        response.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE_RESPONSE);
        response.setCode(RemotingCommandCodeConstants.SUCCESS);
        response.setTransactionId(tsId);
        NettyChannelSendSupporter.sendMessage(response, channel);

        CompletableFuture.runAsync(()->{
            String topic = request.getTopic();
            String queue = request.getQueue();

            byte[] body = request.getBody();


            if(body == null || body.length == 0) {
                log.error("sync save message body is empty, topic[{}], queue[{}], tsId[{}]", topic, queue, topic);
                return;
            }

            TopicInfo topicInfo = topicManager.getTopicInfo(topic);
            Long slaveOffset = topicInfo.getOffset(queue);

            //当前broker 的 offset 小于 master 的 offset，需要同步
            Long masterOffset = request.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);

            if (slaveOffset + body.length < masterOffset) {//有未同步的，需要批量同步
                log.warn("topic[{}]-queue[{}], local offset[{}] < master offset[{}], need sync data",
                        topic, queue, slaveOffset, masterOffset);
                trySyncMsgFromInstance(topic, queue, tsId, slaveOffset, masterOffset, channel);
            } else if (slaveOffset > masterOffset) { //当前offset 大于 master offset， 数据不对，需要删除重新获取
                log.warn("topic[{}]-queue[{}], local offset > master offset, well rebuilt local from master",
                        topic, queue);
                //TODO 停止服务，获取到master数据后再恢复

            } else { //slave 与 master offset同步的，保存该条即可
                log.debug("topic[{}]-queue[{}]-tsId[{}], local offset == master offset, sync append", topic, queue, tsId);
                dispatchMessageService.saveMessage(topic, queue, tsId, slaveOffset, body, channel);
            }
        }, syncMsgToCLusterExecutor);
    }


    /**
     * 开启任务，尝试从master拉取offset到targetOffset之间的消息.期间收到的message会暂存到ShinanoMQConstants.TEMP_TOPIC_PREFIX开头的
     * 暂存topic中。拉取完成后会将暂存的消息放回
     * @param offset offset
     * @param targetOffset targetOffset
     * @param channel channel
     */
    public void trySyncMsgFromInstance(String topic, String queue, String tsId, long offset, long targetOffset, Channel channel) {
        CompletableFuture.runAsync(()->{
            ClusterHost master = nameServerManager.getMaster(springConfig.getServiceId());
            String key = BrokerUtil.makeTopicQueueKey(topic, queue);


            //开启同步消息任务
            syncingQueueMap.computeIfAbsent(key, k->{
                log.warn("create sync message [{}] from master task", key);
                SyncMessageTask task = new SyncMessageTask(k, master, offset, targetOffset);
                syncMsgToCLusterExecutor.execute(task);
                return task;
            });

//            RemotingCommand response = new RemotingCommand();
//            response.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE_RESPONSE);
//            response.setCode(RemotingCommandCodeConstants.SUCCESS);
//            response.setTransactionId(tsId);
//            channel.write(response);
        }, syncMsgToCLusterExecutor);
    }


    @Override
    public void afterPropertiesSet() {
        this.syncingQueueMap = new ConcurrentHashMap<>();
        this.syncMsgToCLusterExecutor = Executors.newFixedThreadPool(10);

        this.selfHost = new ClusterHost(springConfig.getClientId(), springConfig.getAddress(), springConfig.getPort());
    }


    class SyncMessageTask implements Runnable{

        private final String topic;
        private final String queue;
        private final String key;

        private final ClusterHost target;
        private final long startOffset;
        private final long endOffset;
        private long currentOffset;

        private int retry;

        public SyncMessageTask(String key, ClusterHost target, long startOffset, long endOffset) {
            this.key = key;
            Pair<String, String> pair = BrokerUtil.getTopicQueueFromKey(key);
            this.topic = pair.getKey();
            this.queue = pair.getValue();
            this.target = target;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.currentOffset = startOffset;

            this.retry = 0;
        }

        @Override
        public void run() {
            //TODO 最大重试次数
            while (currentOffset < endOffset && retry <= 2) {
                SendCommandFuture future = syncMsgFromInstance(target, topic, queue, currentOffset);
                if (future != null) {
                    Object obj = null;
                    try {
                        obj = future.getResult();
                    } catch (InterruptedException e) {
                        log.error("request sync msg from [{}] error", target);
                    }
                    RemotingCommand response;

                    if (obj != null && !(response = (RemotingCommand) obj).equals(RemotingCommand.TIME_OUT_COMMAND)) {
                        Long offset = response.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);
                        String fileName = response.getExtFieldsValue(ExtFieldsConstants.SAVE_FILE_NAME);
                        Integer length = response.getExtFieldsInt(ExtFieldsConstants.BODY_LENGTH);
                        if (offset == null) {
                            log.error("response of sync msg from [{}] didn't have offset field", target);
                            break;
                        }
                        if (length == 0) break;

                        if (offset == currentOffset) {
                            persistentSupport.persistentBytes(fileName, topic, queue, offset, response.getBody());
                            currentOffset += length;
                            offsetManager.updateTopicQueueOffset(topic, queue, currentOffset);
                        } else if(currentOffset == endOffset) {
                            break;
                        }
                    } else {
                        log.warn("request sync msg retry [{}], topic-queue[{}], remote[{}], current offset[{}], end offset[{}]",
                               retry++, key, target, currentOffset, endOffset);
                    }
                }
            }

            log.warn("sync [{}] message from [{}] finish, start offset [{}], end offset [{}], target end offset [{}]",
                    key, target, startOffset, currentOffset, endOffset);
            //TODO 期间暂存的消息写入
            syncingQueueMap.remove(key);
        }

        private SendCommandFuture syncMsgFromInstance(ClusterHost target, String topic, String queue, long offset) {
            BrokerClusterConnector connector = clusterConnectorManager.getConnector(target);
            RemotingCommand command = RemotingCommandPool.getObject();
            command.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_PULL_MESSAGE);
            command.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
            command.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
            command.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));
            return connector.sendMsgFuture(command);
        }
    }
}
