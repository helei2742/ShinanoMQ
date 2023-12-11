package cn.com.shinano.ShinanoMQ.base.loadbalance;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;

import java.util.Random;

/**
 * @author lhe.shinano
 * @date 2023/12/11
 */
public class ShinanoLoadBalanceRandom implements ShinanoLoadBalance{
    private final Random random = new Random();

    @Override
    public ClusterHost getServerHost() {
        return serverWeightMap.get(random.nextInt(serverWeightMap.size()));
    }
}
