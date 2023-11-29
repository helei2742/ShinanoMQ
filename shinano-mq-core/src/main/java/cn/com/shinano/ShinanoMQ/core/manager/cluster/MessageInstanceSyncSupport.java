package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.NameServerManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Slf4j
@Component
public class MessageInstanceSyncSupport implements InitializingBean {
    private ExecutorService syncMsgToCLusterExecutor = Executors.newFixedThreadPool(2);

    private ClusterHost selfHost;

    @Autowired
    private NameServerManager nameServerManager;

    @Autowired
    private BrokerSpringConfig springConfig;

    @Autowired
    private BrokerClusterConnectorManager clusterConnectorManager;

    public CompletableFuture<PutMessageStatus> syncMsgToInstance(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            List<ClusterHost> instances = nameServerManager.getInstanceList(springConfig.getServiceId());
            RemotingCommand remotingCommand = RemotingCommandPool.getObject();
            remotingCommand.setTransactionId(message.getTransactionId());
            remotingCommand.setFlag(RemotingCommandFlagConstants.BROKER_ONLY_SAVE_MESSAGE);
            remotingCommand.addExtField(ExtFieldsConstants.TOPIC_KEY, message.getTopic());
            remotingCommand.addExtField(ExtFieldsConstants.QUEUE_KEY, message.getQueue());
            message.setTopic(null);
            message.setQueue(null);
            message.setTransactionId(null);
            remotingCommand.setBody(ProtostuffUtils.serialize(message));


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

    @Override
    public void afterPropertiesSet() throws Exception {
        this.selfHost = new ClusterHost(springConfig.getClientId(), springConfig.getAddress(), springConfig.getPort());
    }
}
