package cn.com.shinano.ShinanoMQ.test.controller;

import cn.com.shinano.ShinanoMQ.test.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private ProducerService producerService;

    @GetMapping("/send")
    public Object sendTest() {
        Object send = producerService.send();
        return send;
    }
}
