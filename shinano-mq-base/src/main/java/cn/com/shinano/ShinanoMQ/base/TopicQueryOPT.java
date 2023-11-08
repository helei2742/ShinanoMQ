package cn.com.shinano.ShinanoMQ.base;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
public class TopicQueryOPT {
    public final static String TOPIC_QUERY_OPT_KEY = "topic_query_opt";

    /**
     * 查询 topic-queue 的 offset
     */
    public final static String QUERY_TOPIC_QUEUE_OFFSET = "1";
    public final static String QUERY_TOPIC_QUEUE_OFFSET_RESULT = "-1";

    /**
     * 查询 topic-queue-offset之后的消息
     */
    public final static String QUERY_TOPIC_QUEUE_OFFSET_MESSAGE = "2";
    public final static String QUERY_TOPIC_QUEUE_OFFSET_MESSAGE_RESULT = "-2";
}
