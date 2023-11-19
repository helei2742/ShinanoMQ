package cn.com.shinano.ShinanoMQ.consmer.config;

public class ConsumerConfig {

    /**
     * 消费者心跳间隔
     */
    public static final Integer IDLE_TIME_SECONDS = 30;

    /**
     * 提前拉消息的数据限制
     */
    public static final Integer PRE_PULL_MESSAGE_COUNT = 100;

    /**
     * 提前拉消息的间隔
     */
    public static final long PRE_PULL_MESSAGE_INTERVAL = 5000;

    /**
     * 客户端发成功ack的批大小
     */
    public static final int CONSUME_ACK_BATCH_SIZE = 100;
}
