package cn.com.shinano.nameserver.util;

import io.netty.util.HashedWheelTimer;
import io.netty.util.TimerTask;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
public class TimeWheelUtil {

    private final static HashedWheelTimer hashedWheelTimer =
            new HashedWheelTimer(Executors.defaultThreadFactory(), 1000L, TimeUnit.MILLISECONDS, 512, true,-1L,Executors.newFixedThreadPool(1));

    public static HashedWheelTimer getTimer() {
        return hashedWheelTimer;
    }

    public static void newTimeout(TimerTask task, long time, TimeUnit timeUnit) {
        hashedWheelTimer.newTimeout(task, time, timeUnit);
    }
}
