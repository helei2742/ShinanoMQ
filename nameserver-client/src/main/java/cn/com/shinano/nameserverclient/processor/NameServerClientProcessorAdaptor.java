package cn.com.shinano.nameserverclient.processor;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.ServiceRegistryDTO;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
public class NameServerClientProcessorAdaptor extends AbstractNettyProcessorAdaptor {

    private ClusterHost clusterHost;

    private ServiceRegistryDTO serviceRegistryDTO;

    public NameServerClientProcessorAdaptor(ServiceRegistryDTO serviceRegistryDTO) {
        this.clusterHost = new ClusterHost(serviceRegistryDTO.getClientId(), serviceRegistryDTO.getAddress(), serviceRegistryDTO.getPort());
        this.serviceRegistryDTO = serviceRegistryDTO;
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.debug("name server client get command [{}] ", remotingCommand);
        String tsId = remotingCommand.getTransactionId();

        switch (remotingCommand.getFlag()) {
            case RemotingCommandFlagConstants.CLIENT_DISCOVER_SERVICE_RESPONSE:
            case RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE_RESPONSE:
                resultCallBackInvoker.invokeCallBack(tsId, remotingCommand);
                break;
        }
    }

    @Override
    public void printLog(String logStr) {
        log.debug(logStr);
    }

    @Override
    protected void handleAllIdle(ChannelHandlerContext ctx) {
        sendPingMsg(ctx);
    }

    @Override
    public void sendPingMsg(ChannelHandlerContext context) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.BROKER_PING);
        remotingCommand.setBody(ProtostuffUtils.serialize(this.serviceRegistryDTO));
        sendMsg(context, remotingCommand);
        printLog(String.format("send ping msg [%s] to [%s], hear beat count [%d]",
                context.channel().remoteAddress(), remotingCommand, super.heartbeatCount++));
    }

    public void sendPingMsg(Channel channel) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.BROKER_PING);
        remotingCommand.setBody(ProtostuffUtils.serialize(this.serviceRegistryDTO));
        NettyChannelSendSupporter.sendMessage(remotingCommand, channel);
    }
}
