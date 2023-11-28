package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.manager.NameServerManager;
import cn.com.shinano.nameserverclient.NameServerClient;
import cn.hutool.core.util.RandomUtil;
import io.netty.util.HashedWheelTimer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

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


    @Override
    public void init() {
        log.info("shinano mq broker start connect name server");
        String[] nameservers = brokerSpringConfig.getNameserver();
        nameServerClients = new NameServerClient[nameservers.length];

        for (int i = 0; i < nameservers.length; i++) {
            String nameserver = nameservers[i];

            String[] split = nameserver.split(":");
            NameServerClient nameServerClient = new NameServerClient(brokerSpringConfig.getClientId(), split[0], Integer.parseInt(split[1]));
            nameServerClient.init(brokerSpringConfig.getServiceId(), brokerSpringConfig.getClientId(),
                    brokerSpringConfig.getAddress(), brokerSpringConfig.getPort());
            try {
                nameServerClient.run();
            } catch (InterruptedException e) {
                log.error("run nameserver client error", e);
                System.exit(-1);
            }

            // 只向一个nameserver注册
            if (i == 0) {
                nameServerClient.registryService();
            }
            nameServerClients[i] = nameServerClient;
        }

    }

    @Override
    public void serviceDiscover(String serviceName) {
        nameServerClients[0].discoverService(serviceName);

    }



    @Override
    public ClusterHost getInstance(String serviceName) {
        List<ClusterHost> instance = nameServerClients[0].getInstance(serviceName);
        if (instance == null) {
           return null;
        }
        log.debug("service [{}] have instance list [{}]", serviceName, instance);
        //TODO 客户端负载均衡

        return instance.get(RandomUtil.randomInt(instance.size()));
    }
}
