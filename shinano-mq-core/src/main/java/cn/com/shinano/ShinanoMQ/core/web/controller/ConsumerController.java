package cn.com.shinano.ShinanoMQ.core.web.controller;

import cn.com.shinano.ShinanoMQ.core.web.dto.BrokerRequestDTO;
import cn.com.shinano.ShinanoMQ.core.web.dto.Result;
import cn.com.shinano.ShinanoMQ.core.web.service.ConsumerManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
@RequestMapping("/consumer")
@RestController
public class ConsumerController {

    @Autowired
    private ConsumerManagerService consumerManagerService;

    @PostMapping("/addConsumer")
    public Result addConsumer(@RequestBody BrokerRequestDTO requestDTO) {
        return consumerManagerService.addConsumer(requestDTO);
    }
}
