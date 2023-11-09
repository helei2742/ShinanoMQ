package cn.com.shinano.ShinanoMQ.core.config;


public class BrokerConfig {

    /**
     * 本broker的host
     */
    public static String BROKER_HOST = "localhost:10022";

    /**
     * 处理BoostrapHandler的线程数
     */
    public static final Integer BOOTSTRAP_HANDLER_THREAD = 1;

    /**
     * 持久化文件目录
     */
    public static final String PERSISTENT_FILE_LOCATION = "/Users/helei/develop/ideaworkspace/ShinanoMQ/shinano-mq-core/datalog";
//    public static final String PERSISTENT_FILE_LOCATION = "D:\\develop\\git\\data\\ShinanoMQ\\shinano-mq-core\\datalog";

    /**
     * 单个数据文件大小，单位byte
     */
    public static final Long PERSISTENT_FILE_SIZE = 1024 * 1024L;

    /**
     * 持久化时生成的索引文件等级， 4代表写入一条数据时1/2^3次方概率生成索引
     */
    public static final Integer PERSISTENT_INDEX_LEVEL = 3;

    /**
     * 操作系统一页的大小
     */
    public static int OS_PAGE_SIZE;

    /**
     * 判断客户端下线的时长，单位秒
     */
    public static final Integer CLIENT_OFF_LINE_INTERVAL = 3000;


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
