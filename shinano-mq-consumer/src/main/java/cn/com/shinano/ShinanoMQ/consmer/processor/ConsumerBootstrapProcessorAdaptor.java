package cn.com.shinano.ShinanoMQ.consmer.processor;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerBootstrapProcessorAdaptor extends AbstractNettyProcessorAdaptor {

    @Override
    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
        log.debug("consumer client got message [{}]", remotingCommand);
        switch (remotingCommand.getFlag()) {
            case RemotingCommandFlagConstants.CLIENT_CONNECT_RESULT:
                if(!clientInitMsgProcessor.initClient(remotingCommand.getExtFields())) {
                    eventHandler.initSuccessHandler(remotingCommand);
                }else {
                    eventHandler.initFailHandler();
                }
                break;
            case RemotingCommandFlagConstants.TOPIC_INFO_QUERY_RESULT:
                receiveMessageProcessor.invokeCallBack(remotingCommand.getTransactionId(), remotingCommand);
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
