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

    /**
     * transaction id
     */
    public static final String TRANSACTION_ID_KEY = "transaction_id";

    /**
     * topic name
     */
    public static final String TOPIC_KEY = "topic";

    /**
     * queue name
     */
    public static final String QUEUE_KEY = "queue";

    /**
     * offset
     */
    public static final String OFFSET_KEY = "offset";

    /**
     * 客户端类型
     */
    public static final String CLIENT_TYPE_KEY = "client_type";

    /**
     * producer 客户端
     */
    public static final String CLIENT_TYPE_PRODUCER = "client_producer";

    /**
     * consumer 客户端
     */
    public static final String CLIENT_TYPE_CONSUMER = "client_consumer";
    /**
     * consumer 消费完消息ack状态的key
     */
    public static final String CONSUMER_MIN_ACK_OFFSET_KEY = "consumer_ack_min_offset";
    /**
     * consumer 操作的key
     */
    public static final String CONSUMER_OPT_KEY = "consumer_opt";
    /**
     * consumer 消费完消息的批量ack
     */
    public static final String CONSUMER_BATCH_ACK = "consumer_batch_ack";
    /**
     * consumer 消费完消息的批量ack的响应结果
     */
    public static final String CONSUMER_BATCH_ACK_RESULT = "consumer_batch_ack_result";

    public static final String RETRY_COUNT_KEY = "retry_count";




    public static final String NAMESERVER_VOTE_MASTER = "nameserver_vote_master";
    public static final String NAMESERVER_START_TIMESTAMP = "nameserver_start_timestamp";

    public static final String NAMESERVER_DISCOVER_SERVICE_NAME = "nameserver_discover_service_name";
    public static final String NAMESERVER_LOAD_BALANCE_POLICY = "nameserver_load_balance_policy";

    public static final String BROKER_TYPE = "broker_type";
    public static final String SAVE_FILE_NAME = "save_file_name";
    public static final String BODY_LENGTH = "body_length";

    public static final String HOST_JSON = "host_json";
}
