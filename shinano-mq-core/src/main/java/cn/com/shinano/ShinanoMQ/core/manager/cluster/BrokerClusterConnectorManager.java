package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Slf4j
@Component
public class BrokerClusterConnectorManager {

    private final ConcurrentMap<ClusterHost, BrokerClusterConnector> connectMap;

    @Autowired
    private BrokerSpringConfig springConfig;

    @Autowired
    @Qualifier("selfHost")
    private ClusterHost selfHost;

    BrokerClusterConnectorManager() {
        this.connectMap = new ConcurrentHashMap<>();
    }

    public BrokerClusterConnector getConnector(ClusterHost host) {
        if (selfHost.equals(host)) return null;
        if (host == null) {
            log.debug("param host is null");
            return null;
        }
        return connectMap.compute(host, (k, v) -> {
            if (v == null) {
                try {
                    v = new BrokerClusterConnector(this, host);
                    log.info("connect to cluster node [{}]", host);
                } catch (InterruptedException e) {
                    log.error("create broker cluster connect [{}] error", host, e);
                }
            }
            return v;
        });
    }

    public void removeConnect(ClusterHost connectHost) {
        this.connectMap.remove(connectHost);
    }
}
