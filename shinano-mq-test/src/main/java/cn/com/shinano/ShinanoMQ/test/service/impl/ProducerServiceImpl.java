package cn.com.shinano.ShinanoMQ.test.service.impl;

import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeMessage;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeResultState;
import cn.com.shinano.ShinanoMQ.consumer.ShinanoConsumer;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducer;
import cn.com.shinano.ShinanoMQ.test.service.ProducerService;
import cn.hutool.json.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ProducerServiceImpl implements ProducerService {

    @ShinanoProducer(host = "localhost", port=10022, topic="test-create1", queue = "queue1")
    public Object send() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.set("123", "");
        return jsonObject;
    }

    @ShinanoConsumer(host = "localhost", port=10022, topic="test-create1", queue = "queue1")
    public ConsumeResultState getMessage(ConsumeMessage message) {
        log.info("got response [{}]", message);
        return ConsumeResultState.SUCCESS;
    }
}
