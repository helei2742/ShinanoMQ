package cn.com.shinano.nameserverclient.processor;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
public class NameServerClientProcessorAdaptor extends AbstractNettyProcessorAdaptor {
    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.debug("name server client get command [{}] ", remotingCommand);
    }

    @Override
    public void printLog(String logStr) {
        log.info(logStr);
    }

    @Override
    protected void handleAllIdle(ChannelHandlerContext ctx) {
        sendPingMsg(ctx);
    }
}
