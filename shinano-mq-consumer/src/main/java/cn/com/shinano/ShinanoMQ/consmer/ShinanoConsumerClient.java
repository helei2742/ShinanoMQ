package cn.com.shinano.ShinanoMQ.consmer;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.constant.ClientStatus;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.manager.ConsumerQueueManager;
import cn.com.shinano.ShinanoMQ.consmer.processor.ConsumerBootstrapProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.consmer.processor.ConsumerClientInitProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;


import static cn.com.shinano.ShinanoMQ.base.constant.ClientStatus.CREATE_JUST;
import static cn.com.shinano.ShinanoMQ.base.constant.ClientStatus.RUNNING;


/**
 * @author lhe.shinano
 * @date 2023/11/16
 */
@Slf4j
public class ShinanoConsumerClient extends AbstractNettyClient {

    private final String clientId;

    private ClientStatus status;

    private final ConsumerQueueManager consumerQueueManager;

    public ShinanoConsumerClient(String host, int port, String clientId) {
        super(host, port);
        this.clientId = clientId;
        this.consumerQueueManager = new ConsumerQueueManager();
        this.status = CREATE_JUST;
        this.init();
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

                        status = CREATE_JUST;
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
                if (status == CREATE_JUST) {
                    status = RUNNING;
                } else {
                    log.warn("client [{}] in [{}] state, can not turn to running state", clientId, status);
                }
            }

            @Override
            public void initFailHandler() {

                if (status == CREATE_JUST) {
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
                ConsumerConfig.IDLE_TIME_SECONDS,
                new ReceiveMessageProcessor(),
                new ConsumerClientInitProcessor(),
                new ConsumerBootstrapProcessorAdaptor(),
                nettyClientEventHandler);
    }


    public void queryMessageAfterOffset(String topic,
                                        String queue,
                                        long offset,
                                        int count) {
        RemotingCommand request = new RemotingCommand();
        request.setFlag(RemotingCommandFlagConstants.TOPIC_INFO_QUERY);
        request.addExtField(ExtFieldsConstants.TOPIC_QUERY_OPT_KEY, TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE);
        request.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
        request.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
        request.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));
        request.addExtField(ExtFieldsConstants.QUERY_TOPIC_MESSAGE_COUNT_KEY, String.valueOf(count));


        super.sendMsg(request, remotingCommand -> {
            byte[] body = remotingCommand.getBody();
            MessageListVO vo = ProtostuffUtils.deserialize(body, MessageListVO.class);
            consumerQueueManager.appendMessages(topic, queue, vo);
        }, remotingCommand -> {
            log.error("query message error topic[{}], queue[{}], offset[{}], count[{}]",
                    topic, queue, offset, count);
        });
    }

    public void show() {
        System.out.println(consumerQueueManager.getConsumerQueue());
    }
}
