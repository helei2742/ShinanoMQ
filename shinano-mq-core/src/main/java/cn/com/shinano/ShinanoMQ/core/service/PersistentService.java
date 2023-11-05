package cn.com.shinano.ShinanoMQ.core.service;

public interface PersistentService {
    /**
     * 持久化消息
     * @param id 由服务器生成的消息id
     * @param topic 消息的topic
     * @param queue 消息的queue
     */
    void persistentMessage(long id, String topic, String queue);

    /**
     * 尝试更新topic下queue的offset
     * @param topic 消息的topic
     * @param queue 消息的queue
     * @return
     */
    Long tryQueryOffset(String topic, String queue);
}
