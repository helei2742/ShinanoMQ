package cn.com.shinano.ShinanoMQ.core.web.service.impl;

import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import cn.com.shinano.ShinanoMQ.core.manager.topic.TopicInfo;
import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.dto.BrokerRequestDTO;
import cn.com.shinano.ShinanoMQ.core.web.service.TopicManagerService;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
@Slf4j
@Service
public class TopicManagerServiceImpl implements TopicManagerService {

    @Autowired
    private TopicManager topicManager;

    @Override
    public Result createTopic(BrokerRequestDTO requestDTO) {
        String topic = requestDTO.getTopic();
        if(StrUtil.isBlank(topic)) {
            return Result.fail(Result.ResultCode.PARAMS_ERROR);
        }

        if (!topicManager.createTopic(requestDTO.getTopic(), requestDTO.getQueues())) {
            return Result.fail(Result.ResultCode.TOPIC_CREATE_ERROR);
        }
        log.info("create of update topic [{}] queue [{}]", requestDTO.getTopic(), requestDTO.getQueues());
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
        TopicInfo info = topicManager.getTopicInfo(topic);
        return Result.ok(info);
    }

    @Override
    public Result closeTopic(String topic) {
        if(!topicManager.closeTopic(topic)) {
            return Result.fail(Result.ResultCode.TOPIC_CLOSE_ERROR);
        }

        log.warn("topic [{}] closed!", topic);
        return Result.ok();
    }

    @Override
    public Result recoverTopic(String topic) {
        if(!topicManager.recoverTopic(topic)) {
            return Result.fail(Result.ResultCode.TOPIC_RECOVER_ERROR);
        }
        return Result.ok();
    }

    @Override
    public Result deleteQueues(String topic, List<String> queues) {
        List<String> count = topicManager.deleteQueues(topic, queues);

        log.warn("topic[{}] queues [{}] deleted!", topic, queues);
        return Result.ok(count);
    }

    @Override
    public Result deleteTopic(String topic) {
        if(!topicManager.deleteTopic(topic)) {
            return Result.fail(Result.ResultCode.TOPIC_DELETE_ERROR);
        }

        log.warn("topic [{}] deleted!", topic);
        return Result.ok();
    }
}
