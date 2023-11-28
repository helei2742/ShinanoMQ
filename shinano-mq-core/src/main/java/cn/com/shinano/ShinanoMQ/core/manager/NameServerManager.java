package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;

/**
 * @author lhe.shinano
 * @date 2023/11/28
 */
public interface NameServerManager {
    void init();

    void serviceDiscover(String serviceName);

    ClusterHost getInstance(String serviceName);
}
