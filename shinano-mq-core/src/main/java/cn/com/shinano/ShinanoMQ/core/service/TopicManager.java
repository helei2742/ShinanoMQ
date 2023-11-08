package cn.com.shinano.ShinanoMQ.core.service;

import java.util.List;
import java.util.Map;

/**
 * 负责topic的创建修改删除操作。
 * @author lhe.shinano
 * @date 2023/11/8
 */
public interface TopicManager {

    /**
     * 判断topic是否存在
     * @param topic
     * @return
     */
    boolean isTopicExist(String topic);

    boolean isTopicExist(String topic, String queue);

    /**
     * 创建topic,没有传入queue时，默认添加一个名叫default的queue
     * @param topic
     * @param queues
     * @return
     */
    boolean createTopic(String topic, List<String> queues);

    /**
     * 获取当前topic 名字列表
     * @return
     */
    List<String> getTopicList();

    /**
     * 查询topic信息
     * @param topic
     * @return
     */
    Map<String, Object> getTopicInfo(String topic);
}
