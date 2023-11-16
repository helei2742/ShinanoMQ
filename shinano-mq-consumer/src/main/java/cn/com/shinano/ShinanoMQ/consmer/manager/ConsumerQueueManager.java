package cn.com.shinano.ShinanoMQ.consmer.manager;

import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.consmer.dto.QueueData;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class ConsumerQueueManager {
    private final ConcurrentMap<String, List<QueueData>> consumerQueue;

    private final ReentrantLock lock = new ReentrantLock();

    public ConsumerQueueManager() {
        consumerQueue = new ConcurrentHashMap<>();
    }

    public void appendMessages(String topic, String queue, MessageListVO vo) {
        consumerQueue.putIfAbsent(topic, new ArrayList<>());

        lock.lock();
        try {
            List<QueueData> queueDataList = consumerQueue.get(topic);
            boolean f = true;
            for (QueueData queueData : queueDataList) {
                if(queueData.getQueueName().equals(queue)) {
                    f = false;
                    queueData.appendIntoQueue(vo.getMessages());
                    queueData.setNextOffset(vo.getNextOffset());
                }
            }
            if(f) {
                QueueData queueData = new QueueData(queue, vo.getNextOffset(), new LinkedList<>(vo.getMessages()));
                queueDataList.add(queueData);
            }
        } finally {
            lock.unlock();
        }
    }

    public ConcurrentMap<String, List<QueueData>> getConsumerQueue() {
        return consumerQueue;
    }
}
