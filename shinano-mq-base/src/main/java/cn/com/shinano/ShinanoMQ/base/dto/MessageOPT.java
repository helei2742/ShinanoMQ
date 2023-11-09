package cn.com.shinano.ShinanoMQ.base.dto;

public class MessageOPT {

    /**
     * broker ping
     */
    public static final int BROKER_PING = -1;

    /**
     * broker pong
     */
    public static final int BROKER_PONG = -2;

    /**
     * client链接broker
     */
    public static final int CLIENT_CONNECT = 1;


    /**
     * 生产者发消息
     */
    public static final int PRODUCER_MESSAGE = 2;
    /**
     * 生产者发消息的ACK
     */
    public static final int PRODUCER_MESSAGE_ACK = 3;



    /**
     * 查询broker的信息
     */
    public static final int BROKER_INFO_QUERY = 4;
    /**
     * 查询broker的信息返回的结果
     */
    public static final int BROKER_INFO_QUERY_RESULT = 5;



    /**
     * 查询topic内相关的信息
     */
    public static final int TOPIC_INFO_QUERY = 6;
    /**
     *  查询topic内相关的信息返回的结果
     */
    public static final int TOPIC_INFO_QUERY_RESULT = 7;
}
