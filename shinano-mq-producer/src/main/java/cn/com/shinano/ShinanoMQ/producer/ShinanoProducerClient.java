package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constant.ClientStatus;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.protocol.Serializer;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.producer.manager.FinalRetryFailMessageSaveSupport;
import cn.com.shinano.ShinanoMQ.producer.processor.ProducerBootstrapProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.producer.processor.msgprocessor.ProducerClientInitProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

@Slf4j
public class ShinanoProducerClient extends AbstractNettyClient {

    private ClientStatus status;

    private final ClusterHost clientHost;

    private final ConcurrentMap<String, Integer> retryTimesMap;

    public ShinanoProducerClient(String remoteHost, int remotePort, String localHost, int localPort, String clientId) {
        super(remoteHost, remotePort, localHost, localPort);

        this.status = ClientStatus.CREATE_JUST;
        this.clientHost = new ClusterHost(clientId, localHost, localPort);

        this.retryTimesMap = new ConcurrentHashMap<>();

        init();
    }

    public void run() {
        switch (this.status) {
            case CREATE_JUST:
            case START_FAILED:
                try {
                    super.run();
                } catch (InterruptedException e) {
                    log.error("run client got an error", e);
                    this.status = ClientStatus.SHUTDOWN_ALREADY;
                }
                break;
            case RUNNING:
                log.warn("producer client already in running");
                break;
            case SHUTDOWN_ALREADY:
                log.warn("producer client already shut down");
                break;
        }
    }

    public void init() {
        //返回结果处理器
        ReceiveMessageProcessor resultCallBackInvoker = new ReceiveMessageProcessor();
        resultCallBackInvoker.setExpireSeconds(ProducerConfig.SEND_MESSAGE_TIME_OUT_LIMIT);

        super.init(clientHost.getClientId(),
                ProducerConfig.IDLE_TIME_SECONDS,
                resultCallBackInvoker,
                new ProducerClientInitProcessor(),
                new ProducerBootstrapProcessorAdaptor(),
                new DefaultNettyEventClientHandler() {
                    @Override
                    protected void sendInitMessage(ChannelHandlerContext ctx) {
                        RemotingCommand remotingCommand = new RemotingCommand();
                        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_CONNECT);
                        remotingCommand.addExtField(ExtFieldsConstants.CLIENT_ID_KEY, clientHost.getClientId());
                        remotingCommand.addExtField(ExtFieldsConstants.CLIENT_TYPE_KEY, ExtFieldsConstants.CLIENT_TYPE_PRODUCER);

                        NettyChannelSendSupporter.sendMessage(remotingCommand, ctx.channel());
                    }
                });
    }


    public void sendMessage(String topic, String queue, Object value, Consumer<RemotingCommand> success) {
        sendMessage(UUID.randomUUID().toString(), topic, queue, Serializer.Algorithm.Protostuff.serialize(value), success, null);
    }

    public void sendMessage(String topic, String queue, Object value, Consumer<RemotingCommand> success, Consumer<RemotingCommand> fail) {
        sendMessage(UUID.randomUUID().toString(), topic, queue, Serializer.Algorithm.Protostuff.serialize(value), success, fail);
    }


    private void sendMessage(String transactionId, String topic, String queue, byte[] value, Consumer<RemotingCommand> success, Consumer<RemotingCommand> fail) {

        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.PRODUCER_MESSAGE);
        remotingCommand.addExtField(ExtFieldsConstants.TRANSACTION_ID_KEY, transactionId);
        remotingCommand.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
        remotingCommand.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
        remotingCommand.setBody(value);
        sendMessage(remotingCommand, success, fail);
    }

    public void sendMessage(RemotingCommand request, Consumer<RemotingCommand> success, Consumer<RemotingCommand> fail) {
        if (fail != null) {
            super.sendMsg(request, success, fail);
        } else {
            super.sendMsg(request, success, response -> {
                if (response != null) {
                    if(response.getCode() == RemotingCommandCodeConstants.PARAMS_ERROR) {
                        log.debug("param error");
                        return;
                    }
                    //重试
                    String transactionId = response.getTransactionId();
                    int count = retryTimesMap.getOrDefault(transactionId, 0) + 1;
                    if (count > ProducerConfig.SEND_MESSAGE_RETRY_TIMES) {
                        retryTimesMap.remove(transactionId);
                        FinalRetryFailMessageSaveSupport.appendFinalRetryFailMessage(request, clientHost.toString());
                        log.error("message [{}] retry times out of limit", request);
                    } else {
                        retryTimesMap.put(transactionId, count);
                        sendMessage(request, success, fail);
                    }
                }
            });
        }
    }
}
