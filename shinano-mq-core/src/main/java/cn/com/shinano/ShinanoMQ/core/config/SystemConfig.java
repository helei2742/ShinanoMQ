package cn.com.shinano.ShinanoMQ.core.config;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class SystemConfig {

    public static final String PERSISTENT_FILE_LOCATION = "/Users/helei/develop/ideaworkspace/ShinanoMQ/shinano-mq-core/datalog";

    public static final Long PERSISTENT_FILE_SIZE = 1024 * 10L;

    public static final Integer PERSISTENT_INDEX_LEVEL = 4;

    public static int OS_PAGE_SIZE;

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
