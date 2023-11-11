package cn.com.shinano.ShinanoMQ.producer.config;

import lombok.Data;

@Data
public class ProducerConfig {

    /**
     * 向服务端发送心跳的间隔
     */
    public static int IDLE_TIME_SECONDS = 30;

    /**
     * producer的id
     */
    public static String PRODUCER_CLIENT_ID = "producer-client-1";


    /**
     * 单条消息的最大长度
     */
    public static int SINGLE_MESSAGE_LENGTH = 1024 * 1024;

    /**
     * 单次获取消息的最大数量
     */
    public static int QUERY_MESSAGE_MAX_COUNT = 40;
}
