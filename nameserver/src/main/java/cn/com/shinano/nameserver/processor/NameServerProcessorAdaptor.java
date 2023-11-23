package cn.com.shinano.nameserver.processor;

import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.nameserver.NameServerService;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.nameserver.dto.ClusterHost;
import cn.com.shinano.nameserver.support.MasterManagerSupport;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Slf4j
@ChannelHandler.Sharable
public class NameServerProcessorAdaptor extends AbstractNettyProcessorAdaptor {
    private NameServerService nameServerService;

    public NameServerProcessorAdaptor(NameServerService nameServerService, NettyClientEventHandler eventHandler) {
        super();
        this.nameServerService = nameServerService;
        super.init(new NameServerInitProcessor(),
                new ReceiveMessageProcessor(),
                eventHandler);
    }


    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.info("[{}] get message [{}] from [{}]",
                nameServerService.getClientId(), remotingCommand, context.channel().remoteAddress());

        switch (remotingCommand.getFlag()) {
            case RemotingCommandFlagConstants.NAMESERVER_VOTE_MASTER:
                ClusterHost voteMaster = MasterManagerSupport.tryVoteMaster(nameServerService, remotingCommand);
                if (voteMaster != null) {
                    RemotingCommand command = MasterManagerSupport.setMasterRemoteMessageBuilder(nameServerService.getMaster());
                    log.info("set master [{}]", command);
                    context.channel().writeAndFlush(command);
                }
                break;
//            case RemotingCommandFlagConstants.NAMESERVER_SET_MASTER:
//                ClusterHost master = JSON.parseObject(remotingCommand.getExtFieldsValue(ExtFieldsConstants.NAMESERVER_VOTE_MASTER), ClusterHost.class);
//                nameServerService.setMaster(master);
//                nameServerService.setState(NameServerState.RUNNING);
//                break;
        }
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
