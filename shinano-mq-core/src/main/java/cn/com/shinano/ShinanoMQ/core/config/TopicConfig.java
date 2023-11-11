package cn.com.shinano.ShinanoMQ.core.config;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
public class TopicConfig {

    /**
     * 单条消息的最大长度
     */
    public static final int SINGLE_MESSAGE_LENGTH = 1024 * 1024;

    /**
     * 单次获取消息的最大数量
     */
    public static final int QUERY_MESSAGE_MAX_COUNT = 40;
}
