package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.*;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.protocol.Serializer;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.dto.OffsetAndCount;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.*;
import cn.com.shinano.ShinanoMQ.core.manager.topic.TopicInfo;
import cn.com.shinano.ShinanoMQ.core.store.IndexNode;
import cn.com.shinano.ShinanoMQ.core.store.MappedFile;
import cn.com.shinano.ShinanoMQ.core.store.MappedFileManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Slf4j
@Component
public class MessageInstanceSyncSupport implements InitializingBean {

    private static final int SLAVE_TOPIC_EMPTY_SYNC_COUNT_LIMIT = 16;

    private ExecutorService syncMsgToCLusterExecutor;

    private ConcurrentMap<String, SyncMessageTask> syncingQueueMap;

    private Thread syncTopicInfoToMasterThread;

    @Autowired
    @Qualifier("selfHost")
    private ClusterHost selfHost;

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

    @Autowired
    private BrokerClusterTopicOffsetManager clusterTopicOffsetManager;

    @Autowired
    private MappedFileManager mappedFileManager;


    public void commitSlaveTopicInfoAndSendNeedSyncMsg(RemotingCommand request, Channel channel) {
        //更新记录消息
        CompletableFuture<List<String>> updateSlaveTopicInfo = clusterTopicOffsetManager.updateSlaveTopicInfo(request);

        updateSlaveTopicInfo.thenAcceptAsync((list) -> {
            RemotingCommand response = RemotingCommandPool.getObject();

            response.setFlag(RemotingCommandFlagConstants.BROKER_SLAVE_COMMIT_TOPIC_INFO_RESPONSE);
            response.setCode(RemotingCommandCodeConstants.SUCCESS);
            response.setTransactionId(request.getTransactionId());
            response.setBody(Serializer.Algorithm.JSON.serialize(list));

            NettyChannelSendSupporter.sendMessage(response, channel);
        }, syncMsgToCLusterExecutor);
    }


    /**
     * master将消息同步到其它的slave节点
     *
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
                if (instance.equals(selfHost)) continue;

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

    @Deprecated
    public void saveSyncMsgInSlaveLocal(RemotingCommand request, Channel channel) {
        String tsId = request.getTransactionId();

        RemotingCommand response = new RemotingCommand();
        response.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE_RESPONSE);
        response.setCode(RemotingCommandCodeConstants.SUCCESS);
        response.setTransactionId(tsId);
        NettyChannelSendSupporter.sendMessage(response, channel);

        CompletableFuture.runAsync(() -> {
            String topic = request.getTopic();
            String queue = request.getQueue();

            byte[] body = request.getBody();


            if (body == null || body.length == 0) {
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
     *
     * @param offset       offset
     * @param targetOffset targetOffset
     * @param channel      channel
     */
    public void trySyncMsgFromInstance(String topic, String queue, String tsId, long offset, long targetOffset, Channel channel) {
        CompletableFuture.runAsync(() -> {
            ClusterHost master = nameServerManager.getMaster(springConfig.getServiceId());
            String key = BrokerUtil.makeTopicQueueKey(topic, queue);


            //开启同步消息任务
            syncingQueueMap.computeIfAbsent(key, k -> {
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


    /**
     * 查询index file， 结果写入channel返回
     *
     * @param request request
     * @param channel channel
     */
    public void queryIndexFile(RemotingCommand request, Channel channel) {
        CompletableFuture.runAsync(() -> {
            String topic = request.getTopic();
            String queue = request.getQueue();
            Long offset = request.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);
            String indexFileName = request.getExtFieldsValue(ExtFieldsConstants.INDEX_FILE_NAME_KEY);

            MappedFile mappedFile = mappedFileManager.getMappedFileNoCreate(topic, queue, offset);

            RemotingCommand response = RemotingCommandPool.getObject();
            response.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_PULL_INDEX_RESPONSE);
            response.setCode(RemotingCommandCodeConstants.SUCCESS);
            response.setTransactionId(request.getTransactionId());
            try {
                String indexString = "";
                if (mappedFile != null) {
                    indexString = mappedFile.getIndex().getIndexList().stream().map(IndexNode::toSaveString).collect(Collectors.joining());
                } else {
                    Path path = Paths.get(StoreFileUtil.getTopicQueueSaveDir(topic, queue) + File.separator + indexFileName);
                    if (Files.exists(path)) {

                        indexString = String.join("\n", Files.readAllLines(path));

                    }
                }
                response.setBody(Serializer.Algorithm.Protostuff.serialize(indexString));
            } catch (Exception e) {
                log.error("read index file [{}] context error", indexFileName, e);
                response.setCode(RemotingCommandCodeConstants.FAIL);
            }

            NettyChannelSendSupporter.sendMessage(response, channel);
        }, syncMsgToCLusterExecutor);
    }

    @Override
    public void afterPropertiesSet() {
        this.syncingQueueMap = new ConcurrentHashMap<>();
        this.syncMsgToCLusterExecutor = ExecutorManager.syncMsgToCLusterExecutor;
    }


    /**
     * 开启定时任务，向master发送topic的offset信息，master收到后会将
     * 需要同步的topic返回，得到返回后开启新的线程任务，进行同步
     */
    public void slaveSyncTopicInfoToMasterStart() {
        if (syncTopicInfoToMasterThread == null) {
            synchronized (this) {
                if (syncTopicInfoToMasterThread == null) {
                    syncTopicInfoToMasterThread = new SyncTopicInfoTask("syncTopicInfoToMasterThread");
                    syncTopicInfoToMasterThread.start();
                }
            }
        }
    }


    /**
     *
     */
    class SyncTopicInfoTask extends Thread {

        private volatile boolean interrupted = false;

        private int emptySyncCount = 0;

        SyncTopicInfoTask(String name) {
            super(name);
        }

        @Override
        public void run() {
            while (!interrupted) {
                try {
                    log.debug("start sync slave topic info to master");

                    ClusterHost master = nameServerManager.getMaster(springConfig.getServiceId());
                    BrokerClusterConnector masterConnect = clusterConnectorManager.getConnector(master);

                    boolean haveResponse = false;
                    if (masterConnect == null) {
                        log.error("can not connect to broker master");
                    } else if (!NameServerManager.SLAVE_KEY.equals(springConfig.getType())) {
                        log.error("broker is not slave, can't sync message from master [{}]", master);
                        break;
                    } else {
                        List<String> topicList = topicManager.getTopicList();
                        List<String> list = new ArrayList<>();
                        for (String topic : topicList) {
                            TopicInfo topicInfo = topicManager.getTopicInfo(topic);
                            Map<String, OffsetAndCount> queueInfo = topicInfo.getQueueInfo();
                            for (Map.Entry<String, OffsetAndCount> entry : queueInfo.entrySet()) {
                                String TQ = BrokerUtil.makeTopicQueueKey(topic, entry.getKey());
                                String s = TQ + BrokerUtil.KEY_SEPARATOR + entry.getValue().getOffset() + BrokerUtil.KEY_SEPARATOR + entry.getValue().getOffset();
                                list.add(s);
                            }
                        }

                        RemotingCommand request = new RemotingCommand();
                        request.setFlag(RemotingCommandFlagConstants.BROKER_SLAVE_COMMIT_TOPIC_INFO);
                        request.addExtField(ExtFieldsConstants.HOST_JSON, JSON.toJSONString(selfHost));
                        request.setBody(Serializer.Algorithm.JSON.serialize(list));

                        /*
                         * master 会返回需要同步数据的queue，放在body里
                         */
                        SendCommandFuture future = masterConnect.sendMsgFuture(request);
                        Object result = future.getResult();
                        RemotingCommand response;
                        if (result != null && (response = (RemotingCommand) result).getCode().equals(RemotingCommandCodeConstants.SUCCESS)) {
                            //lines 就为master返回的需要同步的topic信息
                            List<String> lines = Serializer.Algorithm.JSON.deserializeList(response.getBody(), String.class);

                            if (lines.size() > 0) haveResponse = true;

                            for (String line : lines) {
                                log.debug("slave[{}] sync topic info to master[{}] success, need sync topic data [{}]", selfHost, master, line);
                                String[] split = line.split(BrokerUtil.KEY_SEPARATOR);

                                String topic = split[0];
                                String queue = split[1];
                                long offset = Long.parseLong(split[2]);
                                long masterOffset = Long.parseLong(split[3]);
                                int masterCount = Integer.parseInt(split[4]);
                                topicManager.updateCount(topic, queue, masterCount);
                                trySyncMsgFromInstance(topic, queue, null, offset, masterOffset, null);
                            }
                        }
                    }

                    if (haveResponse) {
                        emptySyncCount = 0;
                    } else {
                        emptySyncCount++;
                    }

                    if (emptySyncCount >= SLAVE_TOPIC_EMPTY_SYNC_COUNT_LIMIT) {
                        log.warn("slave broker sync topic info to master got empty response times than [{}]. sync after [{}] millis",
                                SLAVE_TOPIC_EMPTY_SYNC_COUNT_LIMIT, 30000);
                        LockSupport.parkUntil(this, System.currentTimeMillis() + 30000);
                    } else {
                        LockSupport.parkUntil(this, System.currentTimeMillis() + BrokerConfig.SLAVE_BROKER_SYNC_TOPIC_INFO_TO_MASTER_INTERVAL);
                    }

                } catch (InterruptedException e) {
                    log.warn("slave broker sync topic info to master thread interrupted, shut it now");
                    this.interrupted = true;
                } catch (Exception e) {
                    log.error("slave broker sync topic info to master got an unknown error", e);
                }
            }
        }
    }


    class SyncMessageTask implements Runnable {

        private final String topic;
        private final String queue;
        private final String key;

        private final ClusterHost target;
        private final long startOffset;
        private final long endOffset;
        private long currentOffset;

        private int retry;

        private Set<String> syncIndexNameSet;

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

            this.syncIndexNameSet = new HashSet<>();
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

                    if (obj != null && !(response = (RemotingCommand) obj).equals(RemotingCommand.TIME_OUT_COMMAND)
                            && response.getCode() != RemotingCommandCodeConstants.FAIL) {

                        Long offset = response.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);
                        String fileName = response.getExtFieldsValue(ExtFieldsConstants.SAVE_FILE_NAME);
                        Integer length = response.getExtFieldsInt(ExtFieldsConstants.BODY_LENGTH);

                        if (offset == null) {
                            log.error("response of sync msg from [{}] didn't have offset field", target);
                            break;
                        }
                        if (length == 0) break;

                        if (offset == currentOffset &&
                                PutMessageStatus.APPEND_LOCAL == persistentSupport.persistentBytes(fileName, topic, queue, offset, response.getBody())) {
                            currentOffset += length;
                            offsetManager.updateTopicQueueOffset(topic, queue, currentOffset);
                            if (!syncIndexNameSet.contains(fileName)) {
                                syncIndexFromInstance(target, topic, queue, offset, fileName);
                            }
                        } else if (currentOffset == endOffset) {
                            break;
                        } else {
                            log.error("persistent bytes in local error, fileName[{}], offset[{}],length[{}] error, retry[{}]", fileName, offset, length, retry++);
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

        private void syncIndexFromInstance(ClusterHost target, String topic, String queue, long offset, String indexFileName) {
            syncIndexNameSet.add(indexFileName);

            BrokerClusterConnector connector = clusterConnectorManager.getConnector(target);

            RemotingCommand request = RemotingCommandPool.getObject();
            request.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_PULL_INDEX);
            request.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
            request.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
            request.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));
            String fileName = indexFileName.replace(".dat", ".idx");
            request.addExtField(ExtFieldsConstants.INDEX_FILE_NAME_KEY, fileName);

            SendCommandFuture future = connector.sendMsgFuture(request);

            if (future != null) {
                Object obj = null;
                try {
                    obj = future.getResult();
                    RemotingCommand response;
                    if (obj != null
                            && !(response = (RemotingCommand) obj).equals(RemotingCommand.TIME_OUT_COMMAND)
                            && response.getCode() != RemotingCommandCodeConstants.FAIL) {

                        String deserialize = Serializer.Algorithm.Protostuff.deserialize(response.getBody(), String.class);
//                        String[] indexContent = deserialize.split("\n");
//                        MappedFile mappedFile = mappedFileManager.getMappedFile(topic, queue, offset);
//                        for (String nodeStr : indexContent) {
//                            if (StrUtil.isBlank(nodeStr)) continue;
//                            mappedFile.getIndex().addIndexNode(IndexNode.toIndexNode(nodeStr));
//                        }
                        Path path = Paths.get(StoreFileUtil.getTopicQueueSaveDir(topic, queue) + File.separator + fileName);
                        Files.write(path, deserialize.getBytes(StandardCharsets.UTF_8));

                        log.info("sync file topic[{}]-queue[{}]-[{}] index file success",
                                topic, queue, indexFileName);
                    }
                } catch (Exception e) {
                    log.error("request sync index file [{}] from [{}] error", indexFileName, target);
                    syncIndexNameSet.remove(indexFileName);
                }
            }
        }
    }
}
