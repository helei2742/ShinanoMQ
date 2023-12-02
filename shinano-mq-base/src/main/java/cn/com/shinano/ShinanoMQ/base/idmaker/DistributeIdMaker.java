package cn.com.shinano.ShinanoMQ.base.idmaker;


import cn.com.shinano.ShinanoMQ.base.config.BaseConfig;
import cn.hutool.core.net.NetUtil;

import java.nio.ByteBuffer;
import java.util.UUID;

public interface DistributeIdMaker {
    Algorithm DEFAULT = Algorithm.SnowFlake;

    String nextId();

    enum Algorithm implements DistributeIdMaker {
        SnowFlake{
            private volatile SnowFlakeShortUrl snowFlakeShortUrl;
            @Override
            public String nextId() {
                if(snowFlakeShortUrl == null) {
                    synchronized (this) {
                        long c = ByteBuffer.wrap(BaseConfig.serviceId.getBytes()).getLong()%100000;
                        long m = ByteBuffer.wrap(NetUtil.getLocalMacAddress().getBytes()).getLong()%100000;
                        snowFlakeShortUrl = new SnowFlakeShortUrl(Math.abs(c), Math.abs(m));
                    }
                }
                return String.valueOf(snowFlakeShortUrl.nextId());
            }
        },
        RandomUUID {
            @Override
            public String nextId() {
                return UUID.randomUUID().toString();
            }
        }
    }
}
