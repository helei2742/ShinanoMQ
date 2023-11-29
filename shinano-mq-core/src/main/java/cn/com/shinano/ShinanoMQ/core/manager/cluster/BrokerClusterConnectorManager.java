package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Slf4j
@Component
public class BrokerClusterConnectorManager {

    private ConcurrentMap<ClusterHost, BrokerClusterConnector> connectMap;



    @Autowired
    private BrokerSpringConfig springConfig;

    BrokerClusterConnectorManager() {
        this.connectMap = new ConcurrentHashMap<>();
    }

    public BrokerClusterConnector getConnector(ClusterHost host) {
        return connectMap.compute(host, (k, v) -> {
            if (v == null) {
                try {
                    v = new BrokerClusterConnector(host.getClientId(), host.getAddress(), host.getPort());
                } catch (InterruptedException e) {
                    log.error("create broker cluster connect [{}] error", host, e);
                    v = null;
                }
            }
            return v;
        });
    }

}
