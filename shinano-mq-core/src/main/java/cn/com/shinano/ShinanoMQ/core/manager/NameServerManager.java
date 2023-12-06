package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserverclient.NameServerClient;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author lhe.shinano
 * @date 2023/11/28
 */
public interface NameServerManager {
    static final String HOST_TYPE_KEY = "host_type";
    static final String SLAVE_KEY = "slave";
    static final String MASTER_KEY = "master";

    void init(Consumer<NameServerClient> registerCallBack);

    void startServiceDiscover(String serviceName);

    ClusterHost getMaster(String serviceName);

    List<ClusterHost> getSlaveList(String serviceName);
}
