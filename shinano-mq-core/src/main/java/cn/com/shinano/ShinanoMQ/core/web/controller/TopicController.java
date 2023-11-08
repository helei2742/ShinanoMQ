package cn.com.shinano.ShinanoMQ.core.web.controller;

import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.dto.TopicRequestDTO;
import cn.com.shinano.ShinanoMQ.core.web.service.TopicManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    public Result createTopic(@RequestBody TopicRequestDTO requestDTO) {
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
}
