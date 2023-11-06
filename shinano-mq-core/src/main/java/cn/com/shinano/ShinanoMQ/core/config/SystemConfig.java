package cn.com.shinano.ShinanoMQ.core.config;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class SystemConfig {


    /**
     * 处理BoostrapHandler的线程数
     */
    public static final Integer BOOTSTRAP_HANDLER_THREAD = 4;

    /**
     * 持久化文件目录
     */
    public static final String PERSISTENT_FILE_LOCATION = "/Users/helei/develop/ideaworkspace/ShinanoMQ/shinano-mq-core/datalog";

    /**
     * 单个数据文件大小，单位byte
     */
    public static final Long PERSISTENT_FILE_SIZE = 1024 * 10L;

    /**
     * 持久化时生成的索引文件等级， 4代表写入一条数据时1/2^3次方概率生成索引
     */
    public static final Integer PERSISTENT_INDEX_LEVEL = 4;

    /**
     * 操作系统一页的大小
     */
    public static int OS_PAGE_SIZE;

    /**
     * 判断客户端下线的时长，单位秒
     */
    public static final Integer CLIENT_OFF_LINE_INTERVAL = 10;


    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe = null;
            unsafe = (Unsafe)f.get(null);
            OS_PAGE_SIZE = unsafe.pageSize();
        } catch (IllegalAccessException | NoSuchFieldException e) {
            OS_PAGE_SIZE = 1024 * 1024 * 16;
        }
    }
}
