package cn.com.shinano.nameserver.support;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.nameserver.NameServerClusterService;
import cn.com.shinano.nameserver.NameServerServiceConnector;
import cn.com.shinano.nameserver.dto.SendCommandResult;
import cn.com.shinano.nameserver.dto.ServiceRegistryDTO;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.nameserver.NameServerService;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserver.dto.RegistryState;
import cn.com.shinano.nameserver.util.FileUtil;
import cn.com.shinano.nameserver.util.TimeWheelUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author lhe.shinano
 * @date 2023/11/24
 */
@Slf4j
public class ServiceRegistrySupport {

    private static final ExecutorService executor = Executors.newFixedThreadPool(2);

    private static ConcurrentMap<String, Set<ClusterHost>> registeredService;

    private static NameServerService nameServerService;

    public static void init(NameServerService nameServerService) {
        ServiceRegistrySupport.nameServerService = nameServerService;
        //TODO Test use delete it
        FileUtil.clientId = nameServerService.getClientId();

        try {
            registeredService = FileUtil.loadRegistryInfoFromDisk();
            //添加心跳检测
            for (String serviceId : registeredService.keySet()) {
                Set<ClusterHost> hosts = registeredService.get(serviceId);
                for (ClusterHost host : hosts) {
                    NameServerServiceConnector.tryConnectService(host);
                }
            }
        } catch (IOException e) {
            log.error("load disk registry info error", e);
        }
    }


    /**
     * nemeserver 的使用者进行服务注册
     * @param request
     * @return
     */
    public static CompletableFuture<RegistryState> clientServiceRegistry(RemotingCommand request) {
        return CompletableFuture.supplyAsync(() -> {
            return validateRegistryRequest(request);
        }).thenApply(ServiceRegistrySupport::registryLocal)
                .thenApplyAsync(ServiceRegistrySupport::broadCastOrForward, executor)
                .handle(((serviceRegistryDTO, throwable) -> {
                    if (throwable != null) {
                        if (serviceRegistryDTO.getRegistryState() == RegistryState.APPEND_LOCAL) {
                           return RegistryState.APPEND_LOCAL;
                        }
                        return RegistryState.UNKNOW_ERROR;
                    }
                    return serviceRegistryDTO.getRegistryState();
                }));
    }

    /**
     * 验证注册请求，转换为ServiceRegistryDTO对象
     *
     * @param request
     * @return
     */
    public static ServiceRegistryDTO validateRegistryRequest(RemotingCommand request) {
        ServiceRegistryDTO registryDTO = ProtostuffUtils.deserialize(request.getBody(), ServiceRegistryDTO.class);
        String serviceId = registryDTO.getServiceId();

        String clientId = registryDTO.getClientId();
        String address = registryDTO.getAddress();
        Integer port = registryDTO.getPort();
        if (serviceId == null || serviceId.equals("") || clientId == null || clientId.equals("")
                || address == null || address.equals("") || port == null || port <= 0) {
            registryDTO.setRegistryState(RegistryState.PARAM_ERROR);
            return registryDTO;
        }
        registryDTO.setRegistryState(RegistryState.VALIDATE_ACCESS);
        return registryDTO;
    }

    /**
     * 注册到本地
     *
     * @param registryDTO
     * @return
     */
    public static ServiceRegistryDTO registryLocal(ServiceRegistryDTO registryDTO) {
        switch (registryDTO.getRegistryState()) {
            case VALIDATE_ACCESS:
                Set<ClusterHost> set = registeredService.getOrDefault(registryDTO.getServiceId(), new HashSet<>());
                ClusterHost clusterHost = new ClusterHost(registryDTO.getClientId(), registryDTO.getAddress(), registryDTO.getPort());
                set.add(clusterHost);
                registeredService.put(registryDTO.getServiceId(), set);
                registryDTO.setRegistryState(RegistryState.APPEND_LOCAL);
                
                //持久化
                try {
                    FileUtil.saveRegistryInfoToDisk(registeredService);
                } catch (IOException e) {
                    log.error("save registry info to disk error");
                }


                NameServerServiceConnector.tryConnectService(clusterHost);

                return registryDTO;
            case PARAM_ERROR:
            default:
                return registryDTO;
        }
    }

    private static void addServiceHeartBeatCheck(String serviceId, ClusterHost clusterHost) {
        TimeWheelUtil.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                NameServerClusterService client = nameServerService.getClusterConnectMap().get(clusterHost);

            }
        }, 500, TimeUnit.MICROSECONDS);
    }

    public static ServiceRegistryDTO broadCastOrForward(ServiceRegistryDTO registryDTO) {
        //如果当前不是master转发给master
        if (nameServerService.getMaster().equals(nameServerService.getServerHost())) {
            registryDTO.setRegistryState(RegistryState.BROADCAST_SLAVE);
        } else {
            registryDTO.setRegistryState(RegistryState.FORWARD_MASTER);
        }


        RemotingCommand command;
        switch (registryDTO.getRegistryState()) {
            case BROADCAST_SLAVE: //给slave广播
                command = new RemotingCommand();
                command.setFlag(RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_BROADCAST);
                command.setBody(ProtostuffUtils.serialize(registryDTO));

                Integer count = nameServerService.broadcastCommand(command);

                int total = nameServerService.getClusterHosts().size();
                log.debug("broadcast count [{}]. total count [{}]", count, total);
                if (count == 0 && total != 0) {
                    registryDTO.setRegistryState(RegistryState.BROADCAST_SLAVE_FAIL);
                } else {
                    registryDTO.setRegistryState(RegistryState.OK);
                }
                break;

            case FORWARD_MASTER: // 转发给master
                command = new RemotingCommand();
                command.setFlag(RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_FORWARD);
                command.setBody(ProtostuffUtils.serialize(registryDTO));

                SendCommandResult result = nameServerService.sendToMaster(command);
                try {
                    Object o = result.getResult();
                    if (o != null) {
                        RemotingCommand remotingCommand = (RemotingCommand) o;
                        if (remotingCommand.getCode() == RemotingCommandCodeConstants.SUCCESS) {
                            registryDTO.setRegistryState(RegistryState.OK);
                            break;
                        }
                    }
                    registryDTO.setRegistryState(RegistryState.FORWARD_MASTER_FAIL);
                } catch (InterruptedException e) {
                    log.error("forward [{}] to master [{}] error", command, nameServerService.getMaster());
                    registryDTO.setRegistryState(RegistryState.FORWARD_MASTER_FAIL);
                }
                break;
            case PARAM_ERROR:
            default:
                break;
        }
        return registryDTO;
    }

    public static List<ClusterHost> getRegisteredServiceById(String serviceId) {
        ArrayList<ClusterHost> clusterHosts = new ArrayList<>();
        registeredService.computeIfPresent(serviceId, (k,v)->{
            clusterHosts.addAll(v);
            return v;
        });
        return clusterHosts;
    }
}
