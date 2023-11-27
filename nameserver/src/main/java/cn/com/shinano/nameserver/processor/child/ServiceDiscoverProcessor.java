package cn.com.shinano.nameserver.processor.child;

import cn.com.shinano.ShinanoMQ.base.constant.LoadBalancePolicy;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserver.loadbalance.LoadBalanceSupport;
import cn.com.shinano.nameserver.loadbalance.RandomLoadBalanceSupport;
import cn.com.shinano.nameserver.support.ServiceRegistrySupport;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
public class ServiceDiscoverProcessor {

    private static final Map<LoadBalancePolicy, LoadBalanceSupport> policyMap;

    static {
        policyMap = new HashMap<>();
        policyMap.put(LoadBalancePolicy.RANDOM, new RandomLoadBalanceSupport());
    }


    public List<ClusterHost> discoverService(String clientId, String serviceId, LoadBalancePolicy policy) {
        return ServiceRegistrySupport.getRegisteredServiceById(serviceId);
    }
}
