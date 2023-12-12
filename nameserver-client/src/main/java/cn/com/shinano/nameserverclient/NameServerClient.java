package cn.com.shinano.nameserverclient;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.VO.ServiceInstanceVO;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constant.LoadBalancePolicy;
import cn.com.shinano.ShinanoMQ.base.dto.*;
import cn.com.shinano.ShinanoMQ.base.loadbalance.ShinanoLoadBalance;
import cn.com.shinano.ShinanoMQ.base.loadbalance.ShinanoLoadBalanceRoundRobin;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.nameserverclient.config.NameServerClientConfig;
import cn.com.shinano.nameserverclient.processor.NameServerClientProcessorAdaptor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static cn.com.shinano.ShinanoMQ.base.constant.ClientStatus.RUNNING;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
@ChannelHandler.Sharable
public class NameServerClient extends AbstractNettyClient {

    private final String clientId;

    private ClusterHost currentNameserverHost;

    private List<ClusterHost> nameserverHosts;

    private NameServerClientProcessorAdaptor processorAdaptor;

    private ServiceRegistryDTO serviceRegistryDTO;

    private final ConcurrentMap<String, List<RegisteredHost>> serviceInstances;

    private final HashedWheelTimer refreshInstancesTimer;

    private final List<Pair<String, Consumer<List<RegisteredHost>>>> whenDiscoverServices = new ArrayList<>();

    private ShinanoLoadBalance loadBalance;

    private int reconnectNameServerCount;

    @Deprecated
    public NameServerClient(String clientId, String remoteHost, int remotePort) {
        super(remoteHost, remotePort);
        this.clientId = clientId;
        this.currentNameserverHost = new ClusterHost(null, remoteHost, remotePort);
        this.serviceInstances = new ConcurrentHashMap<>();
        this.refreshInstancesTimer = new HashedWheelTimer(Executors.defaultThreadFactory(), 1L, TimeUnit.SECONDS, 512, true, -1L, Executors.newFixedThreadPool(1));
    }

    public NameServerClient(String clientId, List<ClusterHost> nameserverHosts) {
        super(nameserverHosts.get(0).getAddress(), nameserverHosts.get(0).getPort());
        this.clientId = clientId;
        this.nameserverHosts = nameserverHosts;
        this.currentNameserverHost = nameserverHosts.get(0);
        this.serviceInstances = new ConcurrentHashMap<>();
        this.refreshInstancesTimer = new HashedWheelTimer(Executors.defaultThreadFactory(), 1L, TimeUnit.SECONDS, 512, true, -1L, Executors.newFixedThreadPool(1));

        loadBalance = new ShinanoLoadBalanceRoundRobin();
        loadBalance.registryServerHosts(nameserverHosts);
    }

    public NameServerClient whenDiscoverService(String serviceId, Consumer<List<RegisteredHost>> whenDiscoverService) {
        this.whenDiscoverServices.add(new Pair<>(serviceId, whenDiscoverService));
        return this;
    }

    public NameServerClient init(String serviceId, String type, String registryId, String registryAddress, int registryPort) {

        ServiceRegistryDTO serviceRegistryDTO = new ServiceRegistryDTO(serviceId, type, null);
        serviceRegistryDTO.setClientId(registryId);
        serviceRegistryDTO.setAddress(registryAddress);
        serviceRegistryDTO.setPort(registryPort);
        this.serviceRegistryDTO = serviceRegistryDTO;

        processorAdaptor = new NameServerClientProcessorAdaptor(serviceRegistryDTO);

        super.init(clientId,
                NameServerClientConfig.SERVICE_HEART_BEAT_TTL,
                new ReceiveMessageProcessor(),
                prop -> false,
                processorAdaptor,
                new DefaultNettyEventClientHandler() {
                    @Override
                    protected void sendInitMessage(ChannelHandlerContext ctx) {
                        status = RUNNING;
                        reconnectNameServerCount = 0;
                    }

                    @Override
                    public void closeHandler(Channel channel) {
                        //关闭后，如果有其它的host，尝试重新链接
                        tryReconnect();
                    }
                });

        return this;
    }

    /**
     * 尝试重新链接到nameserver
     */
    public void tryReconnect() {
        if (reconnectNameServerCount++ >= 16) {
            log.error("try reconnect nameserver fail [{}] times", reconnectNameServerCount);
            return;
        }

        currentNameserverHost = loadBalance.getServerHost();
        try {
            log.warn("try reconnect to nameserver [{}], retry times [{}]", currentNameserverHost, reconnectNameServerCount);
            reConnect(currentNameserverHost.getAddress(), currentNameserverHost.getPort());
        } catch (InterruptedException e) {
            log.error("reconnect to nameserver [{}] error", currentNameserverHost);
        }
    }

    /**
     * 服务注册，按照init()方法传入的clientId，clientAddress，clientPort进行注册
     *
     * @param successCallBack 成功回调
     */
    public void registryService(Consumer<RemotingCommand> successCallBack) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE);
        remotingCommand.setBody(ProtostuffUtils.serialize(serviceRegistryDTO));
        sendMsg(remotingCommand, success -> {
            successCallBack.accept(success);
            log.info("service registry success, result command [{}]", success);
        }, fail -> log.error("service registry fail, request [{}]", remotingCommand));

        processorAdaptor.sendPingMsg(channel);
    }

    /**
     * 服务祖册
     *
     * @param serviceRegistryDTO 需要注册的服务
     * @param successCallBack    成功回调
     */
    public void registryService(ServiceRegistryDTO serviceRegistryDTO, Consumer<RemotingCommand> successCallBack) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE);
        remotingCommand.setBody(ProtostuffUtils.serialize(serviceRegistryDTO));
        sendMsg(remotingCommand, success -> {
            successCallBack.accept(success);
            log.info("service registry success, result command [{}]", success);
        }, fail -> log.error("service registry fail, request [{}]", remotingCommand));

        processorAdaptor.sendPingMsg(channel);
    }


    /**
     * 服务发现
     *
     * @param serviceId 需要的服务id
     */
    public void discoverService(String serviceId) {
        if (serviceInstances.containsKey(serviceId)) return;

        refreshDiscoverInstances(serviceId);
    }

    private void refreshDiscoverInstances(String serviceId) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_DISCOVER_SERVICE);
        remotingCommand.addExtField(ExtFieldsConstants.NAMESERVER_LOAD_BALANCE_POLICY, LoadBalancePolicy.RANDOM.value);
        remotingCommand.addExtField(ExtFieldsConstants.NAMESERVER_DISCOVER_SERVICE_NAME, serviceId);
        log.debug("try discover service [{}], command [{}]", serviceId, remotingCommand);

        sendMsg(remotingCommand, success -> {
            ServiceInstanceVO instanceVO = ProtostuffUtils.deserialize(success.getBody(), ServiceInstanceVO.class);
            log.info("discover service [{}] success, result [{}]", serviceId, instanceVO);


            serviceInstances.compute(serviceId, (k, v) -> {
                refreshInstancesTimer.newTimeout(timeout ->
                        refreshDiscoverInstances(serviceId), NameServerClientConfig.SERVICE_INSTANCE_REFRESH_INTERVAL, TimeUnit.SECONDS);

                for (Pair<String, Consumer<List<RegisteredHost>>> pair : whenDiscoverServices) {
                    if (pair.getKey().equals(serviceId)) {
                        pair.getValue().accept(instanceVO.getInstances());
                    }
                }

                return instanceVO.getInstances();
            });

        }, fail -> {
            log.error("discover service [{}] fail, request [{}]", serviceId, remotingCommand);
            refreshInstancesTimer.newTimeout(timeout ->
                    refreshDiscoverInstances(serviceId), NameServerClientConfig.SERVICE_INSTANCE_REFRESH_INTERVAL, TimeUnit.SECONDS);
        });
    }

    public List<RegisteredHost> getInstance(String serviceId) {
        return serviceInstances.get(serviceId);
    }
}
