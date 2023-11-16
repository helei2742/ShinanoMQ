package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constant.ClientStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.producer.processor.ProducerBootstrapProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.producer.processor.msgprocessor.ProducerClientInitProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

@Slf4j
public class ShinanoProducerClient extends AbstractNettyClient {

    private ClientStatus status;

    private final String clientId;

    private final ConcurrentMap<String, Integer> retryTimesMap;


    public ShinanoProducerClient(String host, int port, String clientId) {
        super(host, port);

        this.status = ClientStatus.CREATE_JUST;
        this.clientId = clientId;

        this.retryTimesMap = new ConcurrentHashMap<>();

        init();
    }

    public void run()  {
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
        NettyClientEventHandler nettyClientEventHandler = new NettyClientEventHandler() {
            @Override
            public void activeHandler(ChannelHandlerContext ctx) {
                switch (status) {
                    case CREATE_JUST:
                    case START_FAILED:
                        //发送一条链接消息
                        Message message = new Message();
                        message.setFlag(RemotingCommandFlagConstants.CLIENT_CONNECT);
                        message.setBody(clientId.getBytes(StandardCharsets.UTF_8));
                        ctx.writeAndFlush(message);

                        status = ClientStatus.CREATE_JUST;
                        break;
                    default:
                        log.warn("client [{}] in [{}] state, can not turn to active state", clientId, status);
                }
            }
            @Override
            public void closeHandler() {
                switch (status) {
                    case CREATE_JUST:
                    case RUNNING:
                        status = ClientStatus.SHUTDOWN_ALREADY;
                        break;
                    default:
                        log.warn("client [{}] in [{}] state, can not turn to close state", clientId, status);
                }
            }
            @Override
            public void initSuccessHandler() {
                if (status == ClientStatus.CREATE_JUST) {
                    status = ClientStatus.RUNNING;
                } else {
                    log.warn("client [{}] in [{}] state, can not turn to running state", clientId, status);
                }
            }
            @Override
            public void initFailHandler() {

                if (status == ClientStatus.CREATE_JUST) {
                    status = ClientStatus.START_FAILED;
                } else {
                    log.warn("client [{}] in [{}] state, can not turn to start failed state", clientId, status);
                }
            }
            @Override
            public void exceptionHandler(ChannelHandlerContext ctx, Throwable cause) {
                log.error("producer got an error", cause);
                status = ClientStatus.SHUTDOWN_ALREADY;
            }
        };


        super.init(clientId,
                ProducerConfig.IDLE_TIME_SECONDS,
                new ReceiveMessageProcessor(),
                new ProducerClientInitProcessor(),
                new ProducerBootstrapProcessorAdaptor(),
                nettyClientEventHandler);
    }


    public void sendMessage(String topic, String queue, String value, Consumer<RemotingCommand> success) {
        sendMessage(UUID.randomUUID().toString(), topic, queue, value.getBytes(StandardCharsets.UTF_8), success);
    }

    public void sendMessage(String transactionId, String topic, String queue, byte[] value, Consumer<RemotingCommand> success) {

        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommandFlagConstants.PRODUCER_MESSAGE);
        remotingCommand.addExtField(ExtFieldsConstants.TRANSACTION_ID_KEY, transactionId);
        remotingCommand.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
        remotingCommand.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
        remotingCommand.setBody(value);
        sendMessage(remotingCommand, success);
    }

    public void sendMessage(RemotingCommand remotingCommand, Consumer<RemotingCommand> success) {

        super.sendMsg(remotingCommand, success, remotingCommand1 -> {
                    String transactionId = remotingCommand1.getTransactionId();
                    int count = retryTimesMap.getOrDefault(transactionId, 0) + 1;
                    if (count > ProducerConfig.SEND_MESSAGE_RETRY_TIMES) {
                        retryTimesMap.remove(transactionId);
                        log.error("message [{}] retry times out of limit", remotingCommand);
                    } else {
                        retryTimesMap.put(transactionId, count);
                        sendMessage(remotingCommand, success);
                    }
                });
    }
}
