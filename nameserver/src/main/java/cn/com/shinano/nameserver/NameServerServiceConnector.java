package cn.com.shinano.nameserver;


import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.nameserver.config.NameServerConstants;
import cn.com.shinano.nameserver.dto.RegisteredHost;
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

    public static Map<RegisteredHost, ConnectEntry> channelMap = new ConcurrentHashMap<>();

    /**
     * 设置延时任务，验证host 对应的 channel是否可用
     * @param host
     */
    private static void setExpireTimerToCheck(RegisteredHost host) {
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
        }, NameServerConfig.SERVICE_HEART_BEAT_TTL/3, TimeUnit.SECONDS);
    }

    /**
     * 注册host
     * @param host
     */
    public static void registryConnectListener(RegisteredHost host) {
        channelMap.computeIfAbsent(host, k->{
            setExpireTimerToCheck(host);
            String type = "slave";
            if (host.getProps()!=null) {
                String s = host.getProps().get(NameServerConstants.REGISTERED_HOST_TYPE_KEY);
                type = s == null?type:s;
            }
            return new ConnectEntry(null, false, -1L, type);
        });
    }

    /**
     * 刷新链接
     * @param host
     * @param channel
     */
    public static void refreshConnectChannel(RegisteredHost host, Channel channel) {
        log.debug("refresh connect channel, service host [{}]", host);
        channelMap.computeIfPresent(host, (k,v)->{
            if (v.channel == null) {
                v.channel = channel;
                v.channel.attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).set(host.getHost());
            }
            v.usable = true;
            v.lastPongTimeStamp = System.currentTimeMillis();
            if (host.getProps() != null) {
               v.type = host.getProps().get(NameServerConstants.REGISTERED_HOST_TYPE_KEY);
            }
            setExpireTimerToCheck(host);
            return v;
        });
    }

    /**
     * 判断host是否可用
     * @param host
     * @return
     */
    public static boolean isHostAlive(RegisteredHost  host) {
        ConnectEntry connectEntry = channelMap.get(host);
        if(connectEntry == null) return false;
        return connectEntry.usable;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class ConnectEntry {
        private Channel channel;
        private boolean usable;
        private long lastPongTimeStamp;
        private String type;
    }
}
