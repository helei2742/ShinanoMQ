package cn.com.shinano.nameserver.util;

import io.netty.util.HashedWheelTimer;
import io.netty.util.TimerTask;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
public class TimeWheelUtil {

    private final static ExecutorService taskExecutor;
    static {
        taskExecutor = Executors.newFixedThreadPool(1);
    }


    public static HashedWheelTimer newTimeout() {
        return new HashedWheelTimer(Executors.defaultThreadFactory(), 1000L, TimeUnit.MILLISECONDS, 512, true, -1L, taskExecutor);
    }
}
