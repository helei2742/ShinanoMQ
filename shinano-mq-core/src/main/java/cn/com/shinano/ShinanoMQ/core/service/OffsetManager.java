package cn.com.shinano.ShinanoMQ.core.service;

/**
 * 管理topic queue的接口
 */
public interface OffsetManager {

    /**
     * 更新topic 下 queue 中已接受消息的offset
     * @param topic topic name
     * @param queue queue name
     * @param offset 当前queue的offset
     */
    void updateTopicQueueOffset(String topic, String queue, long offset);

    /**
     * 查询topic-queue 当前写入的offset
     * @param topic topic name
     * @param queue queue name
     * @return -1代表没有该queue，大于等于0为当前offset大小
     */
    long queryTopicQueueOffset(String topic, String queue);
}
