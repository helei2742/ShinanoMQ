package cn.com.shinano.nameserver.processor;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.nameserver.NameServerService;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.nameserver.dto.ClusterHost;
import cn.com.shinano.nameserver.dto.NameServerState;
import cn.com.shinano.nameserver.support.MasterManagerSupport;
import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Slf4j
@ChannelHandler.Sharable
public class NameServerClientProcessorAdaptor extends AbstractNettyProcessorAdaptor {
    private NameServerService nameServerService;

    public NameServerClientProcessorAdaptor(NameServerService nameServerService, NettyClientEventHandler eventHandler) {
        super.init(new NameServerClientInitProcessor(), new ReceiveMessageProcessor(), eventHandler);
        this.nameServerService = nameServerService;
    }

    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.info("get message [{}]", remotingCommand);
        switch (remotingCommand.getFlag()) {
            case RemotingCommandFlagConstants.NAMESERVER_SET_MASTER:
                ClusterHost master = JSON.parseObject(remotingCommand.getExtFieldsValue(ExtFieldsConstants.NAMESERVER_VOTE_MASTER), ClusterHost.class);
                nameServerService.setMaster(master);
                nameServerService.setState(NameServerState.RUNNING);
                break;
        }
    }


    @Override
    protected void handleAllIdle(ChannelHandlerContext ctx) {
        super.handleAllIdle(ctx);
        sendPingMsg(ctx);
    }

    @Override
    public void printLog(String logStr) {
        log.info(logStr);
    }
}
