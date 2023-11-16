package cn.com.shinano.ShinanoMQ.base.constans;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
public class TopicQueryConstants {
    /**
     * 查询 topic-queue 的 offset
     */
    public final static String QUERY_TOPIC_QUEUE_OFFSET = "1";

    /**
     * 查询 topic-queue-offset之后的消息
     */
    public final static String QUERY_TOPIC_QUEUE_OFFSET_MESSAGE = "2";

    /**
     * 查询消息头中，查询的数量限制
     */
    public final static int QUERY_TOPIC_MESSAGE_COUNT_LIMIT = 10000;
}
