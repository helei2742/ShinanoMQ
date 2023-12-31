package cn.com.shinano.ShinanoMQ.consmer.manager;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeMessage;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeResultState;
import cn.com.shinano.ShinanoMQ.consmer.listener.ConsumerOnMsgListener;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class QueueData {
    private String topic;
    private String queueName;
    private long nextOffset;
    private LinkedBlockingQueue<ConsumeMessage> queue;

    private ConsumerQueueManager queueManager;

    private final ReentrantLock lock = new ReentrantLock();
    private long lastAppendTime = -1L;
    private Condition pullMessageCondition;
    private CopyOnWriteArrayList<ConsumerOnMsgListener> listeners;
    private volatile AtomicBoolean isInListening;
    private final static ExecutorService publishExecutor = Executors.newSingleThreadExecutor();
    private final static ExecutorService messageHandlerExecutor = Executors.newFixedThreadPool(2);

    public QueueData(String topic,
                     String queue,
                     Long nextOffset,
                     LinkedBlockingQueue<ConsumeMessage> es) {
        this.topic = topic;
        this.queueName = queue;
        this.nextOffset = nextOffset;
        this.queue = es;
        this.pullMessageCondition = lock.newCondition();
        this.listeners = new CopyOnWriteArrayList<>();
        this.isInListening = new AtomicBoolean(false);
    }

    /**
     * 添加监听消息的listener
     * @param listener listener
     */
    public void addListener(ConsumerOnMsgListener listener) {
        lock.lock();
        try {
            listeners.add(listener);
        } finally {
            lock.unlock();
        }

        if(isInListening.compareAndSet(false, true)) {
            prePullMessage();
            publishExecutor.execute(this::publishGetMessageEvent);
        }
    }
    AtomicInteger counter = new AtomicInteger(0);

    /**
     * 向添加的listener 发布消息
     */
    private void publishGetMessageEvent() {
        int fail = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if(queue.size() < ConsumerConfig.PRE_PULL_MESSAGE_COUNT/4) prePullMessage();

                ConsumeMessage take = queue.poll(incrementInterval(ConsumerConfig.PRE_PULL_MESSAGE_INTERVAL, fail),
                        TimeUnit.MILLISECONDS);

                if(take == null) {
                    fail++;
                    continue;
                }
                fail = 0;
                for (ConsumerOnMsgListener msgListener : listeners) {
                    //执行消息的逻辑使用其它线程池
                    messageHandlerExecutor.execute(()->{
                        log.debug("publish message event, topic[{}]-queue[{}], message[{}]", topic, queueName, take);
                        ConsumeResultState state;
                        try {
                            state = msgListener.successHandler(take);
                            //成功ack
                        } catch (Exception e) {
                            msgListener.failHandler(e);
                            //失败ack
                            state = ConsumeResultState.RETRY;
                        }
                        //给broker发送consume offset
                        queueManager.commitConsumeACK(take, state,this);
                    });
                }
            } catch (InterruptedException e) {
                log.warn("publish message event task interrupted, topic[{}]-queue[{}]", topic, queueName);
                Thread.currentThread().interrupt();
            }catch (Exception e) {
                log.error("publish message event task got an error", e);
            }
        }
    }

    private long incrementInterval(long interval, int count) {
        return Math.min(interval + 500L * count, interval * 10);
    }

    /**
     * 添加预拉取的消息
     * @param messages messages
     * @param nextOffset nextOffset
     */
    public void appendNewMessages(List<SaveMessage> messages, Long nextOffset) {
        lock.lock();
        try {
            if(this.queue == null) this.queue = new LinkedBlockingQueue<>();

            for (int i = 0; i < messages.size(); i++) {
                Long no = null;
                if(i+1<messages.size()){
                    no = messages.get(i+1).getOffset();
                }
                ConsumeMessage message = new ConsumeMessage(messages.get(i), no);
                this.queue.add(message);
            }
            this.nextOffset = nextOffset;
            this.lastAppendTime = System.currentTimeMillis();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 判断是否需要提取拉取新的消息
     * @return 是否能够拉取消息，若返回true，会重置唱一次拉取的时间
     */
    public boolean canPullNew() {
        boolean f = queue.size() < ConsumerConfig.PRE_PULL_MESSAGE_COUNT/4 &&
                System.currentTimeMillis() - lastAppendTime >= ConsumerConfig.PRE_PULL_MESSAGE_INTERVAL;
        if(f) this.lastAppendTime = System.currentTimeMillis();
        return f;
    }

    /**
     * 提前拉取消息
     */
    public void prePullMessage() {
        this.lock.lock();
        try {
            //唤醒在ConsumerQueueManager中执行拉取任务的线程
            pullMessageCondition.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    public void lock() {
        this.lock.lock();
    }
    public void unlock() {
        this.lock.unlock();
    }
}
