package cn.com.shinano.ShinanoMQ.core.manager;

import java.util.concurrent.*;

/**
 * @author lhe.shinano
 * @date 2023/12/12
 */
public class ExecutorManager {
    public static final ExecutorService slaveSyncTopicInfoToMaster = new ThreadPoolExecutor(1, 1,
            300, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2), ThreadFactoryManager.slaveSyncTopicInfoToMaster);

    public static final ExecutorService dispatchMessageExecutor = new ThreadPoolExecutor(5, 10, 300,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(), ThreadFactoryManager.dispatchMessageThreadFactory);

    public static final ExecutorService syncMsgToCLusterExecutor = new ThreadPoolExecutor(5, 10, 300,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), ThreadFactoryManager.syncMsgToCLusterThreadFactory);

    public static final ExecutorService persistentMessageExecutor = new ThreadPoolExecutor(5, 10, 300,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(), ThreadFactoryManager.persistentMessageThreadFactory);

    public static final ExecutorService consumeOffsetExecutor = new ThreadPoolExecutor(1, 5, 300,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000), ThreadFactoryManager.consumeOffsetThreadFactory);

    public static final ExecutorService topicQueryExecutor = new ThreadPoolExecutor(5, 10, 300,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(2000), ThreadFactoryManager.topicQueryThreadFactory);

    public static final ExecutorService springListenerExecutor = new ThreadPoolExecutor(1, 1, 300,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000), ThreadFactoryManager.springListenerThreadFactory);

    public static final ExecutorService asyncBrokerAckExecutor = new ThreadPoolExecutor(2, 2, 300,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000), ThreadFactoryManager.asyncBrokerAckThreadFactory);
}
