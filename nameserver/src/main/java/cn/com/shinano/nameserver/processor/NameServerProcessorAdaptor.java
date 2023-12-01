package cn.com.shinano.nameserver.processor;

import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.VO.ServiceInstanceVO;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constant.LoadBalancePolicy;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.nameserver.NameServerService;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RegistryState;
import cn.com.shinano.ShinanoMQ.base.dto.ServiceRegistryDTO;
import cn.com.shinano.nameserver.NameServerServiceConnector;
import cn.com.shinano.nameserver.config.NameServerConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RegisteredHost;
import cn.com.shinano.nameserver.dto.VoteInfo;
import cn.com.shinano.nameserver.processor.child.NameServerInitProcessor;
import cn.com.shinano.nameserver.processor.child.ServiceDiscoverProcessor;
import cn.com.shinano.nameserver.support.MasterManagerSupport;
import cn.com.shinano.nameserver.support.ServiceRegistrySupport;
import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Slf4j
@ChannelHandler.Sharable
public class NameServerProcessorAdaptor extends AbstractNettyProcessorAdaptor {
    private NameServerService nameServerService;

    private ServiceDiscoverProcessor serviceDiscoverProcessor = new ServiceDiscoverProcessor();

    public NameServerProcessorAdaptor(NameServerService nameServerService, NettyClientEventHandler eventHandler) {
        super();
        super.useRemotingCommandPool = false;
        this.nameServerService = nameServerService;
        super.init(new NameServerInitProcessor(),
                new ReceiveMessageProcessor(),
                eventHandler);
    }


    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.info("[{}] get message [{}] from [{}]",
                nameServerService.getClientId(), remotingCommand, context.channel().remoteAddress());
        String tsId = remotingCommand.getTransactionId();
        String clientId = remotingCommand.getClientId();

        RemotingCommand response = null;
        CompletableFuture<RegistryState> result = null;
        switch (remotingCommand.getFlag()) {
            case RemotingCommandFlagConstants.NAMESERVER_VOTE_MASTER:   //投票消息

                ClusterHost selected = JSON.parseObject(remotingCommand.getExtFieldsValue(ExtFieldsConstants.NAMESERVER_VOTE_MASTER), ClusterHost.class);
                Long startTimeStamp = remotingCommand.getExtFieldsLong(ExtFieldsConstants.NAMESERVER_START_TIMESTAMP);
                VoteInfo voteInfo = new VoteInfo(startTimeStamp, selected);

                nameServerService.freshClusterService(selected);

                ClusterHost voteMaster = MasterManagerSupport.tryVoteMaster(nameServerService, voteInfo);

                if (voteMaster != null) {
                    response = MasterManagerSupport.setMasterRemoteMessageBuilder(nameServerService.getMaster());
                    log.info("set master [{}]", response);
                }
                break;

            case RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_FORWARD: //slave收到服务注册消息后，转发给master进行广播或者自己是master进行广播
                result = ServiceRegistrySupport.clientServiceRegistry(remotingCommand);
                response = new RemotingCommand();
                response.setFlag(RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_FORWARD_RESPONSE);
                response.setTransactionId(tsId);

                try {
                    RegistryState registryState = result.get();
                    if (registryState == RegistryState.OK) {
                        response.setCode(RemotingCommandCodeConstants.SUCCESS);
                    } else if(registryState == RegistryState.APPEND_LOCAL) { //只保存到了本地广播，转发或广播失败
                        //TODO 重试
                    }else {
                        response.setCode(RemotingCommandCodeConstants.FAIL);
                    }
                    log.debug("registry state [{}]", registryState);

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                break;

            case RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_BROADCAST: //收到master广播的服务注册消息，直接保存
                response = new RemotingCommand();
                response.setFlag(RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_BROADCAST_RESPONSE);
                response.setTransactionId(tsId);

                ServiceRegistryDTO dto = ServiceRegistrySupport.registryLocal(ServiceRegistrySupport.validateRegistryRequest(remotingCommand));
                log.info("registry service result [{}]", dto);
                if (dto.getRegistryState() == RegistryState.APPEND_LOCAL) {
                    response.setCode(RemotingCommandCodeConstants.SUCCESS);
                } else {
                    response.setCode(RemotingCommandCodeConstants.FAIL);
                }
                break;

            case RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE:  //收到客户端服务注册的请求
                result = ServiceRegistrySupport.clientServiceRegistry(remotingCommand);
                response = new RemotingCommand();
                response.setFlag(RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE_RESPONSE);
                response.setTransactionId(tsId);

                try {
                    RegistryState registryState = result.get();
                    if (registryState == RegistryState.OK){
                        response.setCode(RemotingCommandCodeConstants.SUCCESS);
                    } else {
                        response.setCode(RemotingCommandCodeConstants.FAIL);
                    }
                    log.debug("registry state [{}]", registryState);

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                break;

            case RemotingCommandFlagConstants.CLIENT_DISCOVER_SERVICE:
                String serviceId = remotingCommand.getExtFieldsValue(ExtFieldsConstants.NAMESERVER_DISCOVER_SERVICE_NAME);
                LoadBalancePolicy policy = LoadBalancePolicy.valueOf(remotingCommand.getExtFieldsValue(ExtFieldsConstants.NAMESERVER_LOAD_BALANCE_POLICY));

                response = new RemotingCommand();
                response.setFlag(RemotingCommandFlagConstants.CLIENT_DISCOVER_SERVICE_RESPONSE);
                response.setCode(RemotingCommandCodeConstants.SUCCESS);
                response.setTransactionId(tsId);
                List<RegisteredHost> hosts = serviceDiscoverProcessor.discoverService(clientId, serviceId, policy);
                response.setBody(ProtostuffUtils.serialize(new ServiceInstanceVO(hosts)));

        }

        if(response != null) {
            context.channel().writeAndFlush(response);
        }
    }


    @Override
    protected void handlePing(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.debug("get ping message [{}] from [{}]", remotingCommand, context.channel().remoteAddress());
        //客户端的ping
        if(remotingCommand.getBody() != null && remotingCommand.getBody().length > 0) {
            ClusterHost host = ProtostuffUtils.deserialize(remotingCommand.getBody(), ClusterHost.class);
            RegisteredHost registeredHost = new RegisteredHost(host, new HashMap<>());

            registeredHost.getProps().put(NameServerConstants.REGISTERED_HOST_TYPE_KEY,
                    remotingCommand.getExtFieldsValue(ExtFieldsConstants.BROKER_TYPE));
            NameServerServiceConnector.refreshConnectChannel(registeredHost, context.channel());
        }

        super.handlePing(context, remotingCommand);
    }

    @Override
    protected void handlePong(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        super.handlePong(context, remotingCommand);
    }

    @Override
    protected void handleReaderIdle(ChannelHandlerContext ctx) {
        super.handleReaderIdle(ctx);
        sendPingMsg(ctx);
    }



    @Override
    public void printLog(String logStr) {
        log.info(logStr);
    }
}
