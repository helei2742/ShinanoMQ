package cn.com.shinano.ShinanoMQ.consmer.dto;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class QueueData {
    private String queueName;
    private long nextOffset;
    private LinkedBlockingQueue<SaveMessage> queue;
    private final ReentrantLock lock = new ReentrantLock();
    private long lastAppendTime = -1L;

    public QueueData(String queue, Long nextOffset, LinkedBlockingQueue<SaveMessage> es) {
        this.queueName = queue;
        this.nextOffset = nextOffset;
        this.queue = es;
    }


    public void appendNewMessages(List<SaveMessage> messages, Long nextOffset) {
        lock.lock();
        try {
            if(queue == null) queue = new LinkedBlockingQueue<>();

            queue.addAll(messages);

            this.nextOffset = nextOffset;

            lastAppendTime = System.currentTimeMillis();
        } finally {
            lock.unlock();
        }
    }

    public Pair<SaveMessage, Integer> getMessage() throws InterruptedException {
        return new Pair<>(queue.take(), queue.size());
    }

    public boolean canPullNew() {
        lock.lock();
        try {
            return queue.size() < ConsumerConfig.PRE_PULL_MESSAGE_COUNT &&
                    System.currentTimeMillis() - lastAppendTime >= ConsumerConfig.PRE_PULL_MESSAGE_INTERVAL;
        } finally {
            lock.unlock();
        }
    }

    public void lock() {
        this.lock.lock();
    }
    public void unlock() {
        this.lock.unlock();
    }
}
