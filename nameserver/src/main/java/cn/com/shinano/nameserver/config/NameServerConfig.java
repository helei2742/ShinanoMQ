package cn.com.shinano.nameserver.config;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import io.netty.util.AttributeKey;

import java.io.File;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
public class NameServerConfig {

    public static final AttributeKey<ClusterHost> NETTY_CHANNEL_CLIENT_ID_KEY = AttributeKey.newInstance("remoteClientId");

    public static final int BOOTSTRAP_HANDLER_THREAD = 1;

    public static final String CLIENT_ID = "nameserver-1";

    public static final int SERVICE_OFF_LINE_TTL = 60;

    public static final int MAX_FRAME_LENGTH = 1024*10;

    public static final int SERVICE_HEART_BEAT_TTL = 30;

    public static final int TRY_CONNECT_OTHER_SERVER_MAX_RETRY = 16;

    public static final String REGISTRY_INFO_DISK_PATH = System.getProperty("user.dir") + File.separator + "nameserver" + File.separator + "registryService_%s.info";
}
