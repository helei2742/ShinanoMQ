package cn.com.shinano.ShinanoMQ.core.manager.impl;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RegisteredHost;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.manager.NameServerManager;
import cn.com.shinano.nameserverclient.NameServerClient;
import cn.hutool.core.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
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


    @Override
    public void init() {
        log.info("shinano mq broker start connect name server");
        String[] nameservers = brokerSpringConfig.getNameserver();
        nameServerClients = new NameServerClient[nameservers.length];

        for (int i = 0; i < nameservers.length; i++) {
            String nameserver = nameservers[i];

            String[] split = nameserver.split(":");
            NameServerClient nameServerClient = new NameServerClient(brokerSpringConfig.getClientId(), split[0], Integer.parseInt(split[1]));
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
                nameServerClient.registryService();
            }
            nameServerClients[i] = nameServerClient;
        }

    }

    @Override
    public void serviceDiscover(String serviceName) {
        nameServerClients[0].discoverService(serviceName);

    }


    /**
     * 拉取master
     * @param serviceName
     * @return
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
     * @param serviceName
     * @return
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
