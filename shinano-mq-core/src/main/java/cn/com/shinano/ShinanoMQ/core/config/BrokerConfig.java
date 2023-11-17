package cn.com.shinano.ShinanoMQ.core.config;


import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class BrokerConfig {

    public static final byte[] PERSISTENT_FILE_END_MAGIC = ByteBuffer.allocate(8).putLong(1111111111111111111L).array();

//    public static final String BROKER_TOPIC_INFO_SAVE_PATH = "shinano-mq-core/BrokerTopicInfo.json";
    public static final String BROKER_TOPIC_INFO_SAVE_PATH = "shinano-mq-core\\BrokerTopicInfo.json";

    public static final String BROKER_CONSUMER_INFO_SAVE_PATH = "shinano-mq-core\\BrokerConsumerInfo.json";

    /**
     * 本broker的host
     */
    public static String BROKER_HOST = "localhost:10022";

    /**
     * 处理BoostrapHandler的线程数
     */
    public static final Integer BOOTSTRAP_HANDLER_THREAD = 3;

    /**
     * 查询topic的消息时的线程数
     */
    public static final int TOPIC_QUERY_THREAD = 3;

    /**
     * 持久化文件目录
     */
//    public static final String PERSISTENT_FILE_LOCATION = "/Users/helei/develop/ideaworkspace/ShinanoMQ/shinano-mq-core/datalog";
    public static final String PERSISTENT_FILE_LOCATION = "D:\\develop\\git\\data\\ShinanoMQ\\shinano-mq-core\\datalog";

    /**
     * 单个数据文件大小，单位byte
     */
    public static final Long PERSISTENT_FILE_SIZE = 1024 * 1024L;

    /**
     * 持久化时生成的索引文件等级， 4代表写入一条数据时1/2^3次方概率生成索引
     */
    public static final Integer PERSISTENT_INDEX_LEVEL = 5;

    /**
     * 操作系统一页的大小
     */
    public static int OS_PAGE_SIZE;

    /**
     * 判断客户端下线的时长，单位秒
     */
    public static final Integer CLIENT_OFF_LINE_INTERVAL = 3000;

    /**
     * 持久化到本地需要时间限制
     */
    public static final Integer LOCAL_PERSISTENT_WAIT_TIME_LIMIT = 60;
    /**
     * 持久化到本地需要时间限制的单位
     */
    public static final TimeUnit LOCAL_PERSISTENT_WAIT_TIME_UNIT = TimeUnit.SECONDS;
    /**
     * 超时后重试次数
     */
    public static final Integer LOCAL_PERSISTENT_WAIT_TIME_OUT_RETRY = 3;

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
