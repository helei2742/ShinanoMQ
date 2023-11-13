package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MsgFlagConstants;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.producer.constant.ProducerStatus;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
public class ShinanoProducerClient extends AbstractNettyClient {

    private ProducerStatus status;

    private final String clientId;

    private final ConcurrentMap<String, Integer> retryTimesMap;


    public ShinanoProducerClient(String host, int port, String clientId) {
        super(host, port);
        this.status = ProducerStatus.CREATE_JUST;
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
                    this.status = ProducerStatus.SHUTDOWN_ALREADY;
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
        super.init(clientId, new NettyClientEventHandler() {
            @Override
            public void activeHandler(ChannelHandlerContext ctx) {
                switch (status) {
                    case CREATE_JUST:
                    case START_FAILED:
                        //发送一条链接消息
                        Message message = new Message();
                        message.setFlag(MsgFlagConstants.CLIENT_CONNECT);
                        message.setBody(clientId.getBytes(StandardCharsets.UTF_8));
                        ctx.writeAndFlush(message);

                        status = ProducerStatus.CREATE_JUST;
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
                        status = ProducerStatus.SHUTDOWN_ALREADY;
                        break;
                    default:
                        log.warn("client [{}] in [{}] state, can not turn to close state", clientId, status);
                }
            }

            @Override
            public void initSuccessHandler() {
                if (status == ProducerStatus.CREATE_JUST) {
                    status = ProducerStatus.RUNNING;
                } else {
                    log.warn("client [{}] in [{}] state, can not turn to running state", clientId, status);
                }
            }

            @Override
            public void initFailHandler() {

                if (status == ProducerStatus.CREATE_JUST) {
                    status = ProducerStatus.START_FAILED;
                } else {
                    log.warn("client [{}] in [{}] state, can not turn to start failed state", clientId, status);
                }
            }

            @Override
            public void exceptionHandler(ChannelHandlerContext ctx, Throwable cause) {
                log.error("producer got an error", cause);
                status = ProducerStatus.SHUTDOWN_ALREADY;
            }
        });
    }


    public void sendMessage(String topic, String queue, String value, Consumer<Message> success) {
        sendMessage(UUID.randomUUID().toString(), topic, queue, value.getBytes(StandardCharsets.UTF_8), success);
    }

    public void sendMessage(String transactionId, String topic, String queue, byte[] value, Consumer<Message> success) {
        Message message = new Message();
        message.setFlag(MsgFlagConstants.PRODUCER_MESSAGE);
        message.setTopic(topic);
        message.setQueue(queue);
        message.setBody(value);
        message.setTransactionId(transactionId);

        sendMessage(message, success);
    }

    public void sendMessage(Message message, Consumer<Message> success) {

        super.sendMsg(message, success, msg -> {
                    String transactionId = message.getTransactionId();
                    int count = retryTimesMap.getOrDefault(transactionId, 0) + 1;
                    if (count > ProducerConfig.SEND_MESSAGE_RETRY_TIMES) {
                        retryTimesMap.remove(transactionId);
                        log.error("message [{}] retry times out of limit", message);
                    } else {
                        retryTimesMap.put(transactionId, count);
                        sendMessage(message, success);
                    }
                });
    }
}
