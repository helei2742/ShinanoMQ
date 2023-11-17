package cn.com.shinano.ShinanoMQ.core.web.service.impl;

import cn.com.shinano.ShinanoMQ.core.manager.client.BrokerConsumerInfo;
import cn.com.shinano.ShinanoMQ.core.manager.topic.BrokerTopicInfo;
import cn.com.shinano.ShinanoMQ.core.web.dto.BrokerRequestDTO;
import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.service.ConsumerManagerService;
import cn.hutool.core.util.StrUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
@Service
public class ConsumerManagerServiceImpl implements ConsumerManagerService {

    @Autowired
    private BrokerConsumerInfo brokerConsumerInfo;

    @Autowired
    private BrokerTopicInfo brokerTopicInfo;

    @Override
    public Result addConsumer(BrokerRequestDTO requestDTO) {
        String clientId = requestDTO.getClientId();
        String topic = requestDTO.getTopic();
        List<String> queues = requestDTO.getQueues();

        if (StrUtil.isBlank(clientId) || StrUtil.isBlank(topic) || queues == null || queues.size() == 0) {
            return Result.fail(Result.ResultCode.PARAMS_ERROR);
        }
        List<String> fail = new ArrayList<>();

        for (String queue : queues) {
            if (brokerTopicInfo.isTopicExist(topic, queue)) {
                brokerConsumerInfo.addConsumerInfo(topic, queue, clientId);
            } else {
                fail.add(queue);
            }
        }

        return Result.ok(fail);
    }
}
