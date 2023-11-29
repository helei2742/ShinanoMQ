package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Slf4j
public class BrokerClusterConnector extends AbstractNettyClient {


    public BrokerClusterConnector(String clientId, String host, int port) throws InterruptedException {
        super(host, port);

        super.init(clientId,
                Integer.MAX_VALUE,
                new ReceiveMessageProcessor(),
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
                            case RemotingCommandFlagConstants.BROKER_ONLY_SAVE_MESSAGE_RESPONSE:
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
