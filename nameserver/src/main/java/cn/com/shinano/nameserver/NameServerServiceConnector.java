package cn.com.shinano.nameserver;


import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.nameserver.util.TimeWheelUtil;
import io.netty.channel.*;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


import java.net.ConnectException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;


/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
public class NameServerServiceConnector {

    public static Map<ClusterHost, ConnectEntry> channelMap = new ConcurrentHashMap<>();


    private static void setExpireTimerToCheck(ClusterHost host) {
        //延时，检查是否收到pong 或 ping
        TimeWheelUtil.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                channelMap.computeIfPresent(host, (k, v)->{
                    if (System.currentTimeMillis() - v.getLastPongTimeStamp() >= NameServerConfig.SERVICE_HEART_BEAT_TTL*1000) {
                        log.warn("host [{}] long time no ping/pong since [{}] set shut down it", host, v.getLastPongTimeStamp());
                        v.usable = false;
                        if(v.channel != null) {
//                            v.channel.close();
                            v.channel = null;
                        }
                    };
                    return v;
                });
            }
        }, NameServerConfig.SERVICE_HEART_BEAT_TTL * 2, TimeUnit.SECONDS);
    }


    public static void registryConnectListener(ClusterHost host) {
        channelMap.computeIfAbsent(host, k->{
            setExpireTimerToCheck(host);
            return new ConnectEntry(null, false, -1L);
        });
    }

    public static void refreshConnectChannel(ClusterHost host, Channel channel) {
        log.debug("refresh connect channel, service host [{}]", host);
        channelMap.computeIfPresent(host, (k,v)->{
            if (v.channel == null) {
                v.channel = channel;
                v.channel.attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).set(host);
            }
            v.usable = true;
            v.lastPongTimeStamp = System.currentTimeMillis();

            setExpireTimerToCheck(host);
            return v;
        });
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class ConnectEntry {
        private Channel channel;
        private boolean usable;
        private long lastPongTimeStamp;
    }
}
