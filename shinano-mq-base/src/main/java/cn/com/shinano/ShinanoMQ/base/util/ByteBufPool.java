package cn.com.shinano.ShinanoMQ.base.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
public class ByteBufPool {

    private static PooledByteBufAllocator pool = PooledByteBufAllocator.DEFAULT;

    public static ByteBuf getByteBuf() {
        return pool.buffer();
    }
}
