package cn.com.shinano.nameserver.processor.child;

import cn.com.shinano.ShinanoMQ.base.constant.LoadBalancePolicy;
import cn.com.shinano.ShinanoMQ.base.dto.RegisteredHost;
import cn.com.shinano.nameserver.support.ServiceRegistrySupport;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
public class ServiceDiscoverProcessor {

    public List<RegisteredHost> discoverService(String clientId, String serviceId, LoadBalancePolicy policy) {
        return ServiceRegistrySupport.getRegisteredServiceById(serviceId);
    }
}
