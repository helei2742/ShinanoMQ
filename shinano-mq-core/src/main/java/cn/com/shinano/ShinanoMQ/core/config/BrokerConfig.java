package cn.com.shinano.ShinanoMQ.core.config;


import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class BrokerConfig {
    public static String BROKER_TOPIC_INFO_SAVE_PATH = "shinano-mq-core"+ File.separator+"BrokerTopicInfo.json";

    public static String BROKER_CONSUMER_INFO_SAVE_PATH = "shinano-mq-core"+ File.separator+"BrokerConsumerInfo.json";
    /**
     * 持久化文件目录
     */
    public static String PERSISTENT_FILE_LOCATION = System.getProperty("user.dir") + File.separator + "datalog";

    public static String BROKER_DLQ_MESSAGE_SAVE_PATH = System.getProperty("user.dir") + File.separator + "DLQ";
    public static String BROKER_DLQ_MESSAGE_SAVE_PATTERN = "%s_%s_%s.dat";

    public static String RETRY_LOG_SAVE_DIR = System.getProperty("user.dir") + File.separator + "retrylog";
    public static final String RETRY_LOG_SAVE_FILE_PATTERN = "%s@%s~%s.dat";



    public static final byte[] PERSISTENT_FILE_END_MAGIC = ByteBuffer.allocate(8).putLong(1111111111111111111L).array();

    public static final byte[] MESSAGE_HEADER_MAGIC = ByteBuffer.wrap("shinano".getBytes(StandardCharsets.UTF_8)).array();

    public static final int MESSAGE_HEADER_LENGTH = PERSISTENT_FILE_END_MAGIC.length + MESSAGE_HEADER_MAGIC.length;


    /**
     * broker刷新consumer 消费进度的间隔时间
     */
    public static final long BROKER_CONSUME_OFFSET_FLUSH_INTERVAL = 1000*10;

    public static final int RETRY_CONSUME_MESSAGE_MAX_COUNT = 16;


    /**
     * 本broker的host
     */
    public static String BROKER_HOST = "localhost:10022";

    /**
     * 处理BoostrapHandler的线程数
     */
    public static final int BOOTSTRAP_HANDLER_THREAD = 10;

    /**
     * 查询topic的消息时的线程数
     */
    public static final int TOPIC_QUERY_THREAD = 3;

    /**
     * 单个数据文件大小，单位byte
     */
    public static final long PERSISTENT_FILE_SIZE = 1024;

    /**
     * 同步拉取消息时一次最大长度
     */
    public static final int SYNC_PULL_MAX_LENGTH = 1024 * 1024;


    /**
     * 持久化时生成的索引文件等级， 4代表写入一条数据时1/2^3次方概率生成索引
     */
    public static final int PERSISTENT_INDEX_LEVEL = 6;

    /**
     * 操作系统一页的大小
     */
    public static int OS_PAGE_SIZE;

    /**
     * 判断客户端下线的时长，单位秒
     */
    public static final int CLIENT_OFF_LINE_INTERVAL = 3000;

    /**
     * 持久化到本地需要时间限制
     */
    public static final int LOCAL_PERSISTENT_WAIT_TIME_LIMIT = 60;
    /**
     * 持久化到本地需要时间限制的单位
     */
    public static final TimeUnit LOCAL_PERSISTENT_WAIT_TIME_UNIT = TimeUnit.SECONDS;
    /**
     * 超时后重试次数
     */
    public static final int LOCAL_PERSISTENT_WAIT_TIME_OUT_RETRY = 3;


    public static final long SLAVE_BROKER_SYNC_TOPIC_INFO_TO_MASTER_INTERVAL = 5000;
//    static {
//        try {
//            Field f = Unsafe.class.getDeclaredField("theUnsafe");
//            f.setAccessible(true);
//            Unsafe unsafe = null;
//            unsafe = (Unsafe)f.get(null);
//            OS_PAGE_SIZE = unsafe.pageSize();
//        } catch (IllegalAccessException | NoSuchFieldException e) {
//            OS_PAGE_SIZE = 1024 * 1024 * 16;
//        }
//    }
}
