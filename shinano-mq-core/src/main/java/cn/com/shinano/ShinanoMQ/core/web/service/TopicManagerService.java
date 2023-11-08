package cn.com.shinano.ShinanoMQ.core.web.service;

import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.dto.TopicRequestDTO;

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
}
