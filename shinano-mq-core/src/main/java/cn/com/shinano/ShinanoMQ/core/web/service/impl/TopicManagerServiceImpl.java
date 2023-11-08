package cn.com.shinano.ShinanoMQ.core.web.service.impl;

import cn.com.shinano.ShinanoMQ.core.service.TopicManager;
import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.dto.TopicRequestDTO;
import cn.com.shinano.ShinanoMQ.core.web.service.TopicManagerService;
import cn.hutool.core.util.StrUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
@Service
public class TopicManagerServiceImpl implements TopicManagerService {

    @Autowired
    private TopicManager topicManager;

    @Override
    public Result createTopic(TopicRequestDTO requestDTO) {
        String topic = requestDTO.getTopic();
        if(StrUtil.isBlank(topic)) {
            return Result.fail(Result.ResultCode.PARAMS_ERROR);
        }

        if (!topicManager.createTopic(requestDTO.getTopic(), requestDTO.getQueues())) {
            return Result.fail(Result.ResultCode.TOPIC_CREATE_ERROR);
        }
        return Result.ok();
    }

    @Override
    public Result queryTopicList() {
        List<String> list = topicManager.getTopicList();

        return Result.ok(list);
    }

    @Override
    public Result queryTopicInfo(String topic) {
        if(StrUtil.isBlank(topic)) {
            return Result.fail(Result.ResultCode.PARAMS_ERROR);
        }
        Map<String, Object> map = topicManager.getTopicInfo(topic);
        return Result.ok(map);
    }
}
