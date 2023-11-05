package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.core.dto.BrokerMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * 接受生产者的消息，分发到不同topic到消息阻塞队列
 */
@Service
public class DispatchMessageService {
    private final Map<String, LinkedBlockingQueue<BrokerMessage>> dispatchMap = new ConcurrentHashMap<>();

    @Autowired
    private PersistentService persistentService;

    /**
     * 添加message到对应topic的阻塞队列
     * @param message 服务器收到的消息，加上为其生成的唯一id
     */
    public void addMessageIntoQueue(BrokerMessage message) {
        String topic = message.getMessage().getTopic();
        String queue = message.getMessage().getQueue();

        dispatchMap.putIfAbsent(topic, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<BrokerMessage> bq = dispatchMap.get(topic);
        bq.add(message);

        //持久化
        persistentService.persistentMessage(message.getId(), topic, queue);
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
