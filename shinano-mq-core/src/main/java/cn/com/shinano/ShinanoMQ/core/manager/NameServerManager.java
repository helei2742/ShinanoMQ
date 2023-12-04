package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/28
 */
public interface NameServerManager {
    static final String HOST_TYPE_KEY = "host_type";
    static final String SLAVE_KEY = "slave";
    static final String MASTER_KEY = "master";

    void init();

    void serviceDiscover(String serviceName);

    ClusterHost getMaster(String serviceName);

    List<ClusterHost> getSlaveList(String serviceName);
}
