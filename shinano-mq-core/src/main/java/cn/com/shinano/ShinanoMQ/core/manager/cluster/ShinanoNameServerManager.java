package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RegisteredHost;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.manager.NameServerManager;
import cn.com.shinano.nameserverclient.NameServerClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author lhe.shinano
 * @date 2023/11/28
 */
@Slf4j
@Component
public class ShinanoNameServerManager implements NameServerManager {
    private NameServerClient[] nameServerClients;

    @Autowired
    private BrokerSpringConfig brokerSpringConfig;

    @Autowired
    private BrokerClusterConnectorManager clusterConnectorManager;

    @Override
    public void init(Consumer<NameServerClient> registerCallBack) {
        log.info("shinano mq broker start connect name server");
        String[] nameservers = brokerSpringConfig.getNameserver();
        nameServerClients = new NameServerClient[nameservers.length];

        for (int i = 0; i < nameservers.length; i++) {
            String nameserver = nameservers[i];

            String[] split = nameserver.split(":");
            NameServerClient nameServerClient = new NameServerClient(brokerSpringConfig.getClientId(), split[0], Integer.parseInt(split[1]), this::whenServiceDiscover);
            nameServerClient.init(brokerSpringConfig.getServiceId(), brokerSpringConfig.getType(), brokerSpringConfig.getClientId(),
                    brokerSpringConfig.getAddress(), brokerSpringConfig.getPort());
            try {
                nameServerClient.run();
            } catch (InterruptedException e) {
                log.error("run nameserver client error", e);
                System.exit(-1);
            }

            // 只向一个nameserver注册
            if (i == 0) {
                nameServerClient.registryService(remotingCommand -> {
                    registerCallBack.accept( nameServerClients[0]);
                });
            }
            nameServerClients[i] = nameServerClient;
        }
    }

    @Override
    public void startServiceDiscover(String serviceName) {
        nameServerClients[0].discoverService(serviceName);
    }

    public void whenServiceDiscover(List<RegisteredHost> instances) {
        for (RegisteredHost instance : instances) {
            clusterConnectorManager.getConnector(instance.getHost());
        }
    }

    /**
     * 拉取master
     * @param serviceName serviceName
     * @return master host
     */
    @Override
    public ClusterHost getMaster(String serviceName) {
        List<RegisteredHost> instance = nameServerClients[0].getInstance(serviceName);
        if (instance == null) {
           return null;
        }
        log.debug("service [{}] have instance list [{}]", serviceName, instance);
        return instance.stream().filter(registeredHost -> {
            if(registeredHost.getProps() != null) {
                return MASTER_KEY.equals(registeredHost.getProps().get(HOST_TYPE_KEY));
            }
            return false;
        }).limit(1).collect(Collectors.toList()).get(0).getHost();
    }

    /**
     * 获取slave节点们
     * @param serviceName serviceName
     * @return slave host list
     */
    @Override
    public List<ClusterHost> getSlaveList(String serviceName) {
        List<RegisteredHost> instance = nameServerClients[0].getInstance(serviceName);
        //只拿从节点
        return instance.stream().filter(registeredHost -> {
            if(registeredHost.getProps() != null) {
                return SLAVE_KEY.equals(registeredHost.getProps().get(HOST_TYPE_KEY));
            }
            return true;
        }).map(RegisteredHost::getHost).collect(Collectors.toList());
    }
}
