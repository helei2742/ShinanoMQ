package cn.com.shinano.ShinanoMQ.base.loadbalance;



import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lhe.shinano
 * @date 2023/12/11
 */
public interface ShinanoLoadBalance {

    AtomicInteger pos = new AtomicInteger(0);
    ConcurrentMap<Integer, ClusterHost> serverWeightMap = new ConcurrentHashMap<>();

    ClusterHost  getServerHost();

    default void registryServerHosts(List<ClusterHost> hosts) {
        synchronized (this) {
            int size = serverWeightMap.keySet().size();
            Collection<ClusterHost> values = serverWeightMap.values();
            for (ClusterHost host : hosts) {
                if(values.contains(host)) continue;
                serverWeightMap.put(size++, host);
            }
        }
    }
}
