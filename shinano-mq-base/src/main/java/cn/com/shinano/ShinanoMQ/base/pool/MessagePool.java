package cn.com.shinano.ShinanoMQ.base.pool;

import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Message;


import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class MessagePool {
    private static final int POOL_INIT_SIZE; // 对象池的大小
    private static final int POOL_MAX_SIZE; // 对象池最大的大小
    private static final AtomicIntegerFieldUpdater<MessagePool> COUNT_UPDATER;
    private static final MessagePool INSTANCE;
    private static final Random random = new Random();

    private final PriorityBlockingQueue<PooledMessage> pool;
    private volatile int currentCount = 0;

    static {
        POOL_INIT_SIZE = ShinanoMQConstants.MESSAGE_OBJ_POOL_INIT_SIZE;
        POOL_MAX_SIZE = ShinanoMQConstants.MESSAGE_OBJ_POOL_MAX_SIZE;

        INSTANCE = new MessagePool(new PriorityBlockingQueue<PooledMessage>(POOL_INIT_SIZE,
                (m1,m2)->Boolean.compare(m1.busy, m2.busy)));

        COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MessagePool.class,
                "currentCount");
    }

    private MessagePool(PriorityBlockingQueue<PooledMessage> pool) {
        this.pool = pool;
    }


    public static Message getObject() {
        PooledMessage poll = INSTANCE.pool.poll();

        if(poll == null || poll.isBusy()) {
            poll = new PooledMessage();
        }
        poll.setBusy(true);

        int currentCount = COUNT_UPDATER.get(INSTANCE);
        if(currentCount < POOL_INIT_SIZE) {
            INSTANCE.pool.offer(poll);
            COUNT_UPDATER.incrementAndGet(INSTANCE);
        } else if(currentCount > POOL_INIT_SIZE && currentCount < POOL_MAX_SIZE) {
            if(random.nextBoolean()) {
                INSTANCE.pool.offer(poll);
                COUNT_UPDATER.incrementAndGet(INSTANCE);
            }
        }

        return poll;
    }

    public static void returnObject(Message message) {
        if(message instanceof PooledMessage) {
            message.clear();
            COUNT_UPDATER.decrementAndGet(INSTANCE);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        Map<Message, Integer> countMap = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(100);
        long start = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            new Thread(()->{
                for (int j = 0; j < 1000; j++) {
//                    Message object = MessagePool.getObject();
                    Message object = new Message();
                    countMap.compute(object, (k,v)->{
                        if(v == null) {
                            return 0;
                        } else {
                            return v+1;
                        }
                    });
                    object.setTopic("123" + j);
//                    System.out.println(object .hashCode()+" --- "+object);
//                    try {
//                        TimeUnit.MILLISECONDS.sleep(20);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    object.release();
                }
                latch.countDown();
            }).start();

        }
        latch.await();
        long count = countMap.entrySet().stream().filter(e -> e.getValue() == 1).count();

        System.out.println("total count 1000");
        System.out.println("get once count " + count);
        System.out.println("total get count" + countMap.keySet().size());
        System.out.println("cost time" + (System.currentTimeMillis() - start));
//        MessagePool.getObject();

    }
}


class PooledMessage extends Message{
    boolean busy;

    public PooledMessage() {
        super();
        busy = false;
    }

    public boolean isBusy() {
        return busy;
    }

    public void setBusy(boolean busy) {
        this.busy = busy;
    }

    public void release() {

        MessagePool.returnObject(this);
    }

    public void clear() {
        super.clear();
        this.busy = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PooledMessage)) return false;
        if (!super.equals(o)) return false;
        PooledMessage that = (PooledMessage) o;
        return busy == that.busy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), busy);
    }
}
