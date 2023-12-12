package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.util.ThreadFactoryImpl;

import java.util.concurrent.ThreadFactory;

/**
 * @author lhe.shinano
 * @date 2023/12/12
 */
public class ThreadFactoryManager {

    public static final ThreadFactory slaveSyncTopicInfoToMaster = new ThreadFactoryImpl("slaveSyncTopicInfoToMaster-", true);

    public static final ThreadFactory dispatchMessageThreadFactory = new ThreadFactoryImpl("dispatchMessage-", true);

    public static final ThreadFactory syncMsgToCLusterThreadFactory = new ThreadFactoryImpl("syncMsgToCLuster-", true);

    public static final ThreadFactory persistentMessageThreadFactory = new ThreadFactoryImpl("persistentMessage-", true);

    public static final ThreadFactory consumeOffsetThreadFactory = new ThreadFactoryImpl("consumeOffset-", true);

    public static final ThreadFactory topicQueryThreadFactory = new ThreadFactoryImpl("topicQuery-", true);

    public static final ThreadFactory springListenerThreadFactory = new ThreadFactoryImpl("springListener-", true);

    public static final ThreadFactory asyncBrokerAckThreadFactory = new ThreadFactoryImpl("asyncBrokerAck-", true);


}
