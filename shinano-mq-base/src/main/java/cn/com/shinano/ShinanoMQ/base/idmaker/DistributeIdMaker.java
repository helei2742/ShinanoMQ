package cn.com.shinano.ShinanoMQ.base.idmaker;


import cn.hutool.core.net.NetUtil;

import java.nio.ByteBuffer;
import java.util.UUID;

public interface DistributeIdMaker {
    Algorithm DEFAULT = Algorithm.SnowFlake;

    String nextId(String serviceName);

    enum Algorithm implements DistributeIdMaker {
        SnowFlake{
            private volatile SnowFlakeShortUrl snowFlakeShortUrl;
            @Override
            public String nextId(String serviceName) {
                if(snowFlakeShortUrl == null) {
                    synchronized (this) {
                        long c = serviceName.hashCode()%100000;
                        long m = ByteBuffer.wrap(NetUtil.getLocalMacAddress().getBytes()).getLong()%100000;
                        snowFlakeShortUrl = new SnowFlakeShortUrl(Math.abs(c), Math.abs(m));
                    }
                }
                return String.valueOf(snowFlakeShortUrl.nextId());
            }
        },
        RandomUUID {
            @Override
            public String nextId(String serviceName) {
                return UUID.randomUUID().toString();
            }
        }
    }
}
