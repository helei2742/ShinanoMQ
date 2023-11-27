package cn.com.shinano.nameserverclient;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.nameserverclient.config.NameServerClientConfig;
import cn.com.shinano.nameserverclient.dto.ServiceRegistryDTO;
import cn.com.shinano.nameserverclient.processor.NameServerClientProcessorAdaptor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
public class NameServerClient extends AbstractNettyClient {

    private final String clientId;

    private AbstractNettyProcessorAdaptor processorAdaptor;

    public NameServerClient(String clientId, String host, int port) {
        super(host, port);
        this.clientId = clientId;
    }

    public void init() {

        processorAdaptor = new NameServerClientProcessorAdaptor();

        super.init(clientId,
                NameServerClientConfig.SERVICE_HEART_BEAT_TTL,
                new ReceiveMessageProcessor(),
                new ClientInitMsgProcessor() {
                    @Override
                    public boolean initClient(Map<String, String> prop) {
                        return false;
                    }
                },
                processorAdaptor,
                new DefaultNettyEventClientHandler());
    }

    public void registryService(ServiceRegistryDTO serviceRegistryDTO) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE);
        remotingCommand.setTransactionId(UUID.randomUUID().toString());
        remotingCommand.setBody(ProtostuffUtils.serialize(serviceRegistryDTO));

        sendMsg(remotingCommand, success->{
            log.info("service registry success, result command [{}]", success);
        }, fail->{
            log.error("service registry fail, request [{}]", remotingCommand);
        });
    }
}
