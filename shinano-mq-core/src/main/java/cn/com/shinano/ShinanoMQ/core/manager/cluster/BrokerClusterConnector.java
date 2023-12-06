package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
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

    private final BrokerClusterConnectorManager clusterConnectorManager;

    private final ClusterHost connectHost;

    public BrokerClusterConnector(BrokerClusterConnectorManager clusterConnectorManager, ClusterHost host) throws InterruptedException {
        super(host.getAddress(), host.getPort());

        this.connectHost = host;
        this.clusterConnectorManager = clusterConnectorManager;

        ReceiveMessageProcessor resultCallBackInvoker = new ReceiveMessageProcessor();
        resultCallBackInvoker.setExpireSeconds(60);
        super.init(host.getClientId(),
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
                            case RemotingCommandFlagConstants.BROKER_SLAVE_COMMIT_TOPIC_INFO_RESPONSE:
//                                resultCallBackInvoker.updateExpireTime(remotingCommand.getTransactionId(), 6000);
                                resultCallBackInvoker.invokeCallBack(remotingCommand.getTransactionId(), remotingCommand);
                            default:
                        }
                    }

                    @Override
                    public void handlerRemoved(ChannelHandlerContext ctx) {
                        clusterConnectorManager.removeConnect(connectHost);
                        log.debug("remove host [{}] connect", connectHost);
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
