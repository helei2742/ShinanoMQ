package cn.com.shinano.ShinanoMQ.base.constans;

import io.netty.util.AttributeKey;

public class ShinanoMQConstants {
    /**
     * 放在netty channel 里的 client id 的 key
     */
    public static final AttributeKey<String> ATTRIBUTE_KEY = AttributeKey.valueOf("clientId");

    /**
     * netty frame 的最大长度
     */
    public static final Integer MAX_FRAME_LENGTH = 1024*1024*40;

    public static final int MESSAGE_SIZE_LENGTH = 4;


    /**
     * message对象池初始化大小
     */
    public static final int MESSAGE_OBJ_POOL_INIT_SIZE = 100;
    /**
     * message对象池最大大小
     */
    public static final int MESSAGE_OBJ_POOL_MAX_SIZE = 1000;

    /**
     * broker发送给客户端响应的超时时间
     */
    public static final long BROKER_RESPONSE_TIME_LIMIT = 1000 * 10;

    /**
     * 清理客户端过期的消息回调的间隔时间
     */
    public static final long EXPIRE_CLIENT_HANDLER_CLEAR_INTERVAL = 4000;

    /**
     * 重试队列的开头
     */
    public static final String RETRY_QUEUE_NAME_PREFIX = "%RETRY%";
    /**
     * 死信队列的开头
     */
    public static final String DLQ_BANE_PREFIX = "%DLQ%";
    /**
     * 暂存消息的topic前缀
     */
    public static final String TEMP_TOPIC_PREFIX = "%TEMP%";
}
