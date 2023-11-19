package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;


/**
 * 接受生产者的消息，分发到不同topic到消息阻塞队列
 */
@Slf4j
@Service
public class DispatchMessageService {
    private final Map<String, LinkedBlockingQueue<BrokerMessage>> dispatchMap = new ConcurrentHashMap<>();

    private static final ExecutorService executor = Executors.newFixedThreadPool(2);

    @Autowired
    private PersistentSupport persistentSupport;

    @Autowired
    private BrokerAckManager brokerAckManager;

    @Autowired
    private BrokerSpringConfig brokerSpringConfig;

    /**
     * 添加message到对应topic的阻塞队列
     * @param message 服务器收到的消息，加上为其生成的唯一id
     */
    @Deprecated
    public void addMessageIntoQueue(BrokerMessage message) {
        String topic = message.getMessage().getTopic();
        String queue = message.getMessage().getQueue();

        dispatchMap.putIfAbsent(topic, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<BrokerMessage> bq = dispatchMap.get(topic);
        bq.add(message);

        //持久化
        persistentSupport.persistentMessage(message.getId(), topic, queue);
    }

    /**
     * 直接保存
     * @param message
     * @param channel
     */
    public RemotingCommand saveMessage(Message message, Channel channel) {
        PutMessageResult result;
        if(brokerSpringConfig.getAsyncSendEnable()) {
            CompletableFuture<PutMessageResult> localFuture = persistentSupport.asyncPutMessage(message);

            localFuture.thenAcceptAsync(putMessageResult ->{
                //TODO 保存到其它broker
                RemotingCommand response = handlePutMessageResult(putMessageResult);


                NettyChannelSendSupporter.sendMessage(response, channel);
            }, executor);
            return null;
        } else {
            result = persistentSupport.syncPutMessage(message);

           return handlePutMessageResult(result);
        }
    }

    public RemotingCommand handlePutMessageResult(PutMessageResult result) {
        String tsId = result.getTransactionId();
        RemotingCommand response = RemotingCommandPool.getObject();
        response.setFlag(RemotingCommandFlagConstants.PRODUCER_MESSAGE_RESULT);
        response.setTransactionId(tsId);

        switch (result.getStatus()) {
            case PUT_OK:
                response.setCode(RemotingCommandCodeConstants.SUCCESS);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.PUT_OK.name());
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(RemotingCommandCodeConstants.FAIL);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.FLUSH_DISK_TIMEOUT.name());
                break;
            case CREATE_MAPPED_FILE_FAILED:
                response.setCode(RemotingCommandCodeConstants.FAIL);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.CREATE_MAPPED_FILE_FAILED.name());
                break;
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(RemotingCommandCodeConstants.FAIL);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.PROPERTIES_SIZE_EXCEEDED.name());
                break;
            case UNKNOWN_ERROR:
            default:
                response.setCode(RemotingCommandCodeConstants.FAIL);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.UNKNOWN_ERROR.name());
        }
        return response;
    }



    /**
     * 获取topic对应的阻塞队列
     * @param topic topic
     * @return topic对应的阻塞队列
     */
    public LinkedBlockingQueue<BrokerMessage> getTopicMessageBlockingQueue(String topic) {
        return dispatchMap.get(topic);
    }
}
