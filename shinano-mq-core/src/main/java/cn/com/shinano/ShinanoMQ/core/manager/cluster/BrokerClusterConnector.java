package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Slf4j
public class BrokerClusterConnector extends AbstractNettyClient {


    public BrokerClusterConnector(String clientId, String host, int port) throws InterruptedException {
        super(host, port);

        ReceiveMessageProcessor resultCallBackInvoker = new ReceiveMessageProcessor();
        resultCallBackInvoker.setExpireSeconds(60);
        super.init(clientId,
                Integer.MAX_VALUE,
                resultCallBackInvoker,
                new ClientInitMsgProcessor() {
                    @Override
                    public boolean initClient(Map<String, String> prop) {
                        return false;
                    }
                }, new AbstractNettyProcessorAdaptor() {
                    @Override
                    protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
                        log.info("get message [{}] from [{}]", remotingCommand, context.channel().remoteAddress());
                        switch (remotingCommand.getFlag()) {
                            case RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE_RESPONSE:
                            case RemotingCommandFlagConstants.BROKER_SYNC_PULL_MESSAGE_RESPONSE:
                                resultCallBackInvoker.invokeCallBack(remotingCommand.getTransactionId(), remotingCommand);
                                break;
                            default:
                        }
                    }
                    @Override
                    public void printLog(String logStr) {
                        log.debug(logStr);
                    }
                },
                new DefaultNettyEventClientHandler());

        super.run();
    }

}
