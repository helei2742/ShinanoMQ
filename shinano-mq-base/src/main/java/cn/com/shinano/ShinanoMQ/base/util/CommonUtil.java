package cn.com.shinano.ShinanoMQ.base.util;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;

public class CommonUtil {

    public static ClusterHost clusterHostGenerate(String hostStr) {
        String[] split = hostStr.split(":");
        return new ClusterHost("", split[0], Integer.parseInt(split[1]));
    }
}
