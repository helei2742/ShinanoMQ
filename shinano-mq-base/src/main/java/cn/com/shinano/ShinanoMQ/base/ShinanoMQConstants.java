package cn.com.shinano.ShinanoMQ.base;

import io.netty.util.AttributeKey;

public class ShinanoMQConstants {
    /**
     * 放在netty channel 里的 client id 的 key
     */
    public static final AttributeKey<String> ATTRIBUTE_KEY = AttributeKey.valueOf("clientId");

    /**
     * netty frame 的最大长度
     */
    public static final Integer MAX_FRAME_LENGTH = 1024*1024*40;

    public static final int MESSAGE_SIZE_LENGTH = 4;
}
