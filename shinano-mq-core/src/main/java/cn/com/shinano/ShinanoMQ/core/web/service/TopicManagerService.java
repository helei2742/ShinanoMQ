package cn.com.shinano.ShinanoMQ.core.web.service;

import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.dto.TopicRequestDTO;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
public interface TopicManagerService {


    /**
     * 创建topic
     * @param requestDTO
     * @return
     */
    Result createTopic(TopicRequestDTO requestDTO);

    /**
     * 查询当前topic列表
     * @return
     */
    Result queryTopicList();

    /**
     * 查询topic的信息
     * @param topic
     * @return
     */
    Result queryTopicInfo(String topic);

    /**
     * 关闭topic，不会真正删除
     * @param topic
     * @return
     */
    Result closeTopic(String topic);

    /**
     * 恢复被关闭的topic
     * @param topic
     * @return
     */
    Result recoverTopic(String topic);

    /**
     * 删除topic下的queue
     * @param topic
     * @param queues
     * @return
     */
    Result deleteQueues(String topic, List<String> queues);

    /**
     * 删除topic
     * @param topic
     * @return
     */
    Result deleteTopic(String topic);


}
