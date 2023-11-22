package cn.com.shinano.ShinanoMQ.core.manager.topic;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.RetryMessage;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerRetryMessage;
import cn.com.shinano.ShinanoMQ.core.manager.client.BrokerConsumerInfo;
import cn.com.shinano.ShinanoMQ.core.spring.event.PushIntoDLQEvent;
import cn.com.shinano.ShinanoMQ.core.spring.event.PushIntoRetryQueueEvent;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;



/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
@Service
public class RetryTopicQueueManager implements ApplicationContextAware {

    @Autowired
    private BrokerConsumerInfo consumerInfo;

    private ApplicationContext applicationContext;

    /**
     * 将重试消息放到重试队列中
     * @param clientId clientId
     * @param topic topic
     * @param queue queue
     * @param retryMessage retryMessage
     * @return
     */
    public RemotingCommand pushIntoRetryQueue(String clientId, String topic, String queue, RetryMessage retryMessage) {
        RemotingCommand response = RemotingCommandPool.getObject();
        response.setFlag(RemotingCommandFlagConstants.RETRY_CONSUME_MESSAGE_RESULT);

        if(consumerInfo.isConsumerBindQueue(clientId, topic, queue)) {
            retryMessage.addRetryTimes();
            Message message = new Message();
            message.setTopic(topic);

            if(retryMessage.getRetryTimes() <= BrokerConfig.RETRY_CONSUME_MESSAGE_MAX_COUNT) { //放重试队列
                //添加好必要的标签后，放原来的队列等消费
                message.setRetryTimes(retryMessage.getRetryTimes());
                message.getProperties().put("last_message_offset", String.valueOf(retryMessage.getOffset()));
                message.getProperties().put("last_message_length", String.valueOf(retryMessage.getLength()));
                message.setQueue(queue);
                applicationContext.publishEvent(new PushIntoRetryQueueEvent(message));
            }else { //放死信队列

                message.setBody(ProtostuffUtils.serialize(retryMessage));
                message.setQueue(ShinanoMQConstants.DLQ_BANE_PREFIX + message.getQueue());
                applicationContext.publishEvent(new PushIntoDLQEvent(message));
            }
            response.setCode(RemotingCommandCodeConstants.SUCCESS);
        } else {
            response.setCode(RemotingCommandCodeConstants.FAIL);
        }

        return response;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
