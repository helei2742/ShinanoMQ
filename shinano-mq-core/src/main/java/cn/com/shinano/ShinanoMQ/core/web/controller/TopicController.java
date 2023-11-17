package cn.com.shinano.ShinanoMQ.core.web.controller;

import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.dto.BrokerRequestDTO;
import cn.com.shinano.ShinanoMQ.core.web.service.TopicManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
@RequestMapping("/topic")
@RestController
public class TopicController {


    @Autowired
    private TopicManagerService topicManagerService;

    @PostMapping("/create")
    public Result createTopic(@RequestBody BrokerRequestDTO requestDTO) {
        return topicManagerService.createTopic(requestDTO);
    }

    @PostMapping("/topicList")
    public Result topicList() {
        return topicManagerService.queryTopicList();
    }

    @PostMapping("/topicInfo/{topic}")
    public Result topicInfo(@PathVariable(name = "topic") String topic) {
        return topicManagerService.queryTopicInfo(topic);
    }

    @PostMapping("/closeTopic/{topic}")
    public Result closeTopic(@PathVariable(name = "topic") String topic) {
        return topicManagerService.closeTopic(topic);
    }

    @PostMapping("/recoverTopic/{topic}")
    public Result recoverTopic(@PathVariable(name = "topic") String topic) {
        return topicManagerService.recoverTopic(topic);
    }

    @PostMapping("/deleteTopic/{topic}")
    public Result deleteTopic(@PathVariable(name = "topic") String topic) {
        return topicManagerService.deleteTopic(topic);
    }

    @PostMapping("/deleteQueue/{topic}")
    public Result deleteQueue(@PathVariable(name = "topic") String topic,
                              @RequestBody List<String> queues) {
        return topicManagerService.deleteQueues(topic, queues);
    }
}
