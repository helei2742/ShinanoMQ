package cn.com.shinano.ShinanoMQ.base;

public class MessageOPT {

    /**
     * broker ping
     */
    public static final Integer BROKER_PING = -1;

    /**
     * broker pong
     */
    public static final Integer BROKER_PONG = -2;

    /**
     * client链接broker
     */
    public static final Integer CLIENT_CONNECT = 1;


    /**
     * 生产者发消息
     */
    public static final Integer PRODUCER_MESSAGE = 2;
    /**
     * 生产者发消息的ACK
     */
    public static final Integer PRODUCER_MESSAGE_ACK = 3;



    /**
     * 查询broker的信息
     */
    public static final Integer BROKER_INFO_QUERY = 4;
    /**
     * 查询broker的信息返回的结果
     */
    public static final Integer BROKER_INFO_QUERY_RESULT = 5;




    /**
     * 查询topic-queue offset处往后的消息
     */
    public static final Integer TOPIC_QUEUE_OFFSET_MESSAGE_QUERY = 6;
    /**
     * 查询topic-queue offset处往后的消息的结果
     */
    public static final Integer TOPIC_QUEUE_OFFSET_MESSAGE_QUERY_RESULT = 7;
    /**
     * 查询topic-queue offset处往后的消息,没这个topic queue
     */
    public static final Integer TOPIC_QUEUE_OFFSET_MESSAGE_QUERY_404 = 8;


}
