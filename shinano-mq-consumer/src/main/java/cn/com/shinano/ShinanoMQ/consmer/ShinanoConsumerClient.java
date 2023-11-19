package cn.com.shinano.ShinanoMQ.consmer;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.VO.ConsumerInfoVO;
import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.constant.ClientStatus;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.listener.ConsumerOnMsgListener;
import cn.com.shinano.ShinanoMQ.consmer.manager.ConsumerQueueManager;
import cn.com.shinano.ShinanoMQ.consmer.processor.ConsumerBootstrapProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.consmer.processor.ConsumerClientInitProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * @author lhe.shinano
 * @date 2023/11/16
 */
@Slf4j
public class ShinanoConsumerClient extends AbstractNettyClient {

    private final String clientId;

    private final ConsumerQueueManager consumerQueueManager;

    public ShinanoConsumerClient(String host, int port, String clientId) {
        super(host, port);
        this.clientId = clientId;

        this.consumerQueueManager = new ConsumerQueueManager(this);
        this.init();
    }

    public void init() {
        super.init(clientId,
                ConsumerConfig.IDLE_TIME_SECONDS,
                new ReceiveMessageProcessor(),
                new ConsumerClientInitProcessor(),
                new ConsumerBootstrapProcessorAdaptor(),
                new DefaultNettyEventClientHandler(){
                    @Override
                    protected void sendInitMessage(ChannelHandlerContext ctx) {
                        RemotingCommand remotingCommand = new RemotingCommand();
                        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_CONNECT);
                        remotingCommand.addExtField(ExtFieldsConstants.CLIENT_ID_KEY, clientId);
                        remotingCommand.addExtField(ExtFieldsConstants.CLIENT_TYPE_KEY, ExtFieldsConstants.CLIENT_TYPE_CONSUMER);

                        NettyChannelSendSupporter.sendMessage(remotingCommand, ctx.channel());
                    }

                    @Override
                    protected void clientInit(RemotingCommand remotingCommand) {
                        ConsumerInfoVO vo = ProtostuffUtils.deserialize(remotingCommand.getBody(), ConsumerInfoVO.class);
                        consumerQueueManager.initConsumerInfo(vo.getConsumerInfo());
                        log.info("init consumer client by config [{}]", vo);
                    }
                });
    }


    /**
     * 查询topic queue 下 offset之后的 count条消息
     * @param topic topic
     * @param queue queue
     * @param offset offset
     * @param count count
     */
    public void pullMessageAfterOffset(String topic,
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
            this.consumerQueueManager.appendMessages(topic, queue, vo);
        }, remotingCommand -> {
            log.error("query message error topic[{}], queue[{}], offset[{}], count[{}]",
                    topic, queue, offset, count);
        });
    }


    public void onMessage(String topic, String queue, ConsumerOnMsgListener listener) {
        while (!status.equals(ClientStatus.RUNNING)) {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                log.error("wait for client start error, ", e);
            }
        }

        this.consumerQueueManager.onMessageReceive(topic, queue, listener);
    }

    public String getClientId() {
        return clientId;
    }
}
