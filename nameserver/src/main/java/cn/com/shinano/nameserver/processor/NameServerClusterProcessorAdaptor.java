package cn.com.shinano.nameserver.processor;

import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.nameserver.NameServerService;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserver.dto.NameServerState;
import cn.com.shinano.nameserver.processor.child.NameServerClusterInitProcessor;
import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Slf4j
@ChannelHandler.Sharable
public class NameServerClusterProcessorAdaptor extends AbstractNettyProcessorAdaptor {
    private NameServerService nameServerService;

    public NameServerClusterProcessorAdaptor(NameServerService nameServerService, NettyClientEventHandler eventHandler) {
        super.init(new NameServerClusterInitProcessor(), new ReceiveMessageProcessor(), eventHandler);
        this.nameServerService = nameServerService;
        super.useRemotingCommandPool = false;
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.info("get message [{}]", remotingCommand);
        String tsId = remotingCommand.getTransactionId();

        switch (remotingCommand.getFlag()) {
            case RemotingCommandFlagConstants.NAMESERVER_SET_MASTER:
                ClusterHost master = JSON.parseObject(remotingCommand.getExtFieldsValue(ExtFieldsConstants.NAMESERVER_VOTE_MASTER), ClusterHost.class);
                nameServerService.setMaster(master);
                nameServerService.setState(NameServerState.RUNNING);
                break;
            case RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_FORWARD_RESPONSE:
            case RemotingCommandFlagConstants.NAMESERVER_SERVICE_REGISTRY_BROADCAST_RESPONSE:
                super.resultCallBackInvoker.invokeCallBack(tsId, remotingCommand);
        }
    }


    @Override
    protected void handleAllIdle(ChannelHandlerContext ctx) {
        super.handleAllIdle(ctx);
        sendPingMsg(ctx);
    }

    @Override
    public void printLog(String logStr) {
        log.debug(logStr);
    }
}
