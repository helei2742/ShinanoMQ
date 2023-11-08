package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.core.config.TopicConfig;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerTopicInfo;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
@Slf4j
@Service
public class LocalTopicInfoPersistentService {

    @Autowired
    private BrokerTopicInfo brokerTopicInfo;


    @Scheduled(cron = "0/10 * * * * *")
    public void persistent() {
        log.info("start persistent topic info");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(TopicConfig.BROKER_TOPIC_INFO_SAVE_PATH))){
            bw.write(JSON.toJSONString(brokerTopicInfo));
            log.info("persistent topic info success");
        } catch (IOException e) {
            log.error("persistent topic info error",e);
        }
    }
}
