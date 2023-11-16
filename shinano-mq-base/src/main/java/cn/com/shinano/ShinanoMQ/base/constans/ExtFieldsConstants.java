package cn.com.shinano.ShinanoMQ.base.constans;

public class ExtFieldsConstants {

    /**
     * 客户端的key
     */
    public final static String CLIENT_ID_KEY = "client_id";

    /**
     * 单条消息长度限制的key
     */
    public final static String SINGLE_MESSAGE_LENGTH_KEY = "singe_message_length";

    /**
     * 查询消息最大数量限制的key
     */
    public final static String QUERY_MESSAGE_MAX_COUNT_KEY = "query_message_max_count";

    /**
     * 查询message时，数量的key
     */
    public final static String QUERY_TOPIC_MESSAGE_COUNT_KEY = "topic_query_message_count";

    /**
     * 查询topic的类型key
     */
    public final static String TOPIC_QUERY_OPT_KEY = "topic_query_opt";

    /**
     * 生产者发送消息的结果key
     */
    public final static String PRODUCER_PUT_MESSAGE_RESULT_KEY = "producer_send_message_result";

    public static final String TRANSACTION_ID_KEY = "transaction_id";

    public static final String TOPIC_KEY = "topic";
    public static final String QUEUE_KEY = "queue";
    public static final String OFFSET_KEY = "offset";
}
