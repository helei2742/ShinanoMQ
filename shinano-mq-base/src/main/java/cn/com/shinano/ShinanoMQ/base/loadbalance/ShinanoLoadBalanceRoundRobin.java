package cn.com.shinano.ShinanoMQ.base.loadbalance;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;

/**
 * @author lhe.shinano
 * @date 2023/12/11
 */
public class ShinanoLoadBalanceRoundRobin implements ShinanoLoadBalance {
    @Override
    public ClusterHost getServerHost() {
        pos.set((pos.incrementAndGet())% serverWeightMap.size());
        return serverWeightMap.get(pos.get());
    }
}
