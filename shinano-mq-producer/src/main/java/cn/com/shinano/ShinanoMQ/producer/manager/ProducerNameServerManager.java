package cn.com.shinano.ShinanoMQ.producer.manager;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RegisteredHost;
import cn.com.shinano.nameserverclient.NameServerClient;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author lhe.shinano
 * @date 2023/11/30
 */
@Slf4j
public class ProducerNameServerManager {
    private final NameServerClient nameServerClient;

    private final List<ClusterHost> nameserverHosts;

    private ClusterHost brokerMaster;

    private final Set<ClusterHost> brokerSlave;

    private final AtomicBoolean isSuccessDiscoverBroker = new AtomicBoolean(false);

    public ProducerNameServerManager(String brokerServiceId, String serviceId, ClusterHost registryHost, List<ClusterHost> nameservers) {
        if(nameservers.size() == 0) throw new IllegalArgumentException("nameserver host could not empty");

        this.brokerSlave = new HashSet<>();

        nameserverHosts = nameservers;

        nameServerClient = new NameServerClient(registryHost.getClientId(), nameserverHosts)
                .init(serviceId, null, registryHost.getClientId(), registryHost.getAddress(), registryHost.getPort())
                .whenDiscoverService(brokerServiceId, this::whenDiscoverBroker);

        try {
            nameServerClient.run();
            nameServerClient.registryService(response->log.info("registry service[{}]-client[{}] success", serviceId, registryHost));
            nameServerClient.discoverService(brokerServiceId);
            nameServerClient.discoverService(serviceId);
        } catch (Exception e) {
            log.error("nameserver client [{}] run error, {}", nameserverHosts.get(0), e.getMessage());
        }
    }

    private void whenDiscoverBroker(List<RegisteredHost> registeredHosts) {
        log.debug("discover broker [{}]", registeredHosts);
        if(registeredHosts == null) return;
        isSuccessDiscoverBroker.set(true);
        for (RegisteredHost registeredHost : registeredHosts) {
            if (registeredHost.getProps() != null && RegisteredHost.MASTER_KEY.equals(registeredHost.getProps().get(RegisteredHost.HOST_TYPE_KEY))) {
                brokerMaster = registeredHost.getHost();
            } else {
                brokerSlave.add(registeredHost.getHost());
            }
        }
    }

    public void reConnectNameServer() {
        nameServerClient.tryReconnect();
    }

    public ClusterHost getBrokerAddress() throws InterruptedException {
        waitForDiscoverBroker();
        return brokerMaster;
    }

    public Set<ClusterHost> getSlaveAddress() throws InterruptedException {
        waitForDiscoverBroker();
        return brokerSlave;
    }

    private void waitForDiscoverBroker() throws InterruptedException {
        int times = 0;
        while (!isSuccessDiscoverBroker.get()) {
            TimeUnit.MILLISECONDS.sleep(50);
            if (times++ > 16) break;
        }
    }
}
