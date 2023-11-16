package cn.com.shinano.ShinanoMQ.base.constans;

public class RemotingCommandFlagConstants {

    public static final String REQUEST_ERROR = "request_error";

    /**
     * broker ping
     */
    public static final int BROKER_PING = 1;

    /**
     * broker pong
     */
    public static final int BROKER_PONG = -1;

    /**
     * client链接broker
     */
    public static final int CLIENT_CONNECT = 2;

    /**
     * client链接broker的结果
     */
    public static final int CLIENT_CONNECT_RESULT = -2;


    /**
     * 生产者发消息
     */
    public static final int PRODUCER_MESSAGE = 3;
    /**
     * 生产者发消息的结果
     */
    public static final int PRODUCER_MESSAGE_RESULT = -3;
    /**
     * broker发送给客户端的ack
     */
    public static final int BROKER_MESSAGE_ACK = 4;

    /**
     * broker发送给client的批量ack
     */
    public static final int BROKER_MESSAGE_BATCH_ACK = -4;



    /**
     * 查询broker的信息
     */
    public static final int BROKER_INFO_QUERY = 5;
    /**
     * 查询broker的信息返回的结果
     */
    public static final int BROKER_INFO_QUERY_RESULT = -5;



    /**
     * 查询topic内相关的信息
     */
    public static final int TOPIC_INFO_QUERY = 6;
    /**
     *  查询topic内相关的信息返回的结果
     */
    public static final int TOPIC_INFO_QUERY_RESULT = -6;


}
