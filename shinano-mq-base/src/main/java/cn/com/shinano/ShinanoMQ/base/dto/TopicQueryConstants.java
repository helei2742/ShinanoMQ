package cn.com.shinano.ShinanoMQ.base.dto;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
public class TopicQueryConstants {
    public final static String TOPIC_QUERY_OPT_KEY = "topic_query_opt";

    /**
     * 查询 topic-queue 的 offset
     */
    public final static String QUERY_TOPIC_QUEUE_OFFSET = "1";

    /**
     * 查询 topic-queue-offset之后的消息
     */
    public final static String QUERY_TOPIC_QUEUE_OFFSET_MESSAGE = "2";



    public final static String QUERY_TOPIC_MESSAGE_COUNT_KEY = "topic_query_message_count";
    public final static int QUERY_TOPIC_MESSAGE_COUNT_LIMIT = 20;

}
