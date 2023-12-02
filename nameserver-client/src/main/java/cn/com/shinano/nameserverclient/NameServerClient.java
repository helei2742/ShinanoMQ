package cn.com.shinano.nameserverclient;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.VO.ServiceInstanceVO;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constant.LoadBalancePolicy;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RegisteredHost;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.ServiceRegistryDTO;
import cn.com.shinano.ShinanoMQ.base.idmaker.DistributeIdMaker;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.nameserverclient.config.NameServerClientConfig;
import cn.com.shinano.nameserverclient.processor.NameServerClientProcessorAdaptor;
import io.netty.util.HashedWheelTimer;
import io.netty.util.NetUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
public class NameServerClient extends AbstractNettyClient {

    private final String clientId;

    private AbstractNettyProcessorAdaptor processorAdaptor;

    private ServiceRegistryDTO serviceRegistryDTO;

    private ConcurrentMap<String, List<RegisteredHost>> serviceInstances;

    private HashedWheelTimer refreshInstancesTimer;

    public NameServerClient(String clientId, String remoteHost, int remotePort) {
        super(remoteHost, remotePort);
        this.clientId = clientId;
        this.serviceInstances = new ConcurrentHashMap<>();
        this.refreshInstancesTimer = new HashedWheelTimer(Executors.defaultThreadFactory(), 1L, TimeUnit.SECONDS, 512, true,-1L,Executors.newFixedThreadPool(1));
    }

    public void init(String serviceId, String type, String clientId, String address, int port) {
        ServiceRegistryDTO serviceRegistryDTO = new ServiceRegistryDTO(serviceId, type, null);
        serviceRegistryDTO.setClientId(clientId);
        serviceRegistryDTO.setAddress(address);
        serviceRegistryDTO.setPort(port);
        this.serviceRegistryDTO = serviceRegistryDTO;

        processorAdaptor = new NameServerClientProcessorAdaptor(serviceRegistryDTO);

        super.init(this.clientId,
                NameServerClientConfig.SERVICE_HEART_BEAT_TTL,
                new ReceiveMessageProcessor(),
                new ClientInitMsgProcessor() {
                    @Override
                    public boolean initClient(Map<String, String> prop) {
                        return false;
                    }
                },
                processorAdaptor,
                new DefaultNettyEventClientHandler());
    }

    /**
     * 服务注册
     */
    public void registryService() {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE);
        remotingCommand.setBody(ProtostuffUtils.serialize(serviceRegistryDTO));
        System.out.println(serviceRegistryDTO);
        sendMsg(remotingCommand, success->{
            log.info("service registry success, result command [{}]", success);
        }, fail->{
            log.error("service registry fail, request [{}]", remotingCommand);
        });
    }


    /**
     * 服务发现
     * @param serviceId 需要的服务id
     */
    public void discoverService(String serviceId) {
        if(serviceInstances.containsKey(serviceId)) return;

        refreshInstances(serviceId);
    }

    private void refreshInstances(String serviceId) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_DISCOVER_SERVICE);
        remotingCommand.addExtField(ExtFieldsConstants.NAMESERVER_LOAD_BALANCE_POLICY, LoadBalancePolicy.RANDOM.value);
        remotingCommand.addExtField(ExtFieldsConstants.NAMESERVER_DISCOVER_SERVICE_NAME, serviceId);
        log.debug("try discover service [{}], command [{}]", serviceId, remotingCommand);

        sendMsg(remotingCommand, success->{
            ServiceInstanceVO instanceVO = ProtostuffUtils.deserialize(success.getBody(), ServiceInstanceVO.class);
            log.info("discover service [{}] success, result [{}]", serviceId, instanceVO);

            serviceInstances.compute(serviceId, (k,v)->{
                refreshInstancesTimer.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        refreshInstances(serviceId);
                    }
                }, NameServerClientConfig.SERVICE_INSTANCE_REFRESH_INTERVAL, TimeUnit.SECONDS);

                return instanceVO.getInstances();
            });

        }, fail->{
            log.error("discover service [{}] fail, request [{}]", serviceId, remotingCommand);
        });
    }

    public List<RegisteredHost> getInstance(String serviceId) {
        return serviceInstances.get(serviceId);
    }
}
