package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/28
 */
public interface NameServerManager {
    void init();

    void serviceDiscover(String serviceName);

    ClusterHost getMaster(String serviceName);

    List<ClusterHost> getSlaveList(String serviceName);
}
