package cn.com.shinano.ShinanoMQ.core.spring.factorybean;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.config.TopicConfig;
import cn.com.shinano.ShinanoMQ.core.manager.topic.BrokerTopicInfo;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 读取文件中的topic信息成bean，放入容器
 * @author lhe.shinano
 * @date 2023/11/8
 */
@Slf4j
@Component
public class BrokerTopicInfoFactoryBean extends AbstractFactoryBean<BrokerTopicInfo> {
    @Override
    public Class<?> getObjectType() {
        return BrokerTopicInfo.class;
    }

    @Override
    protected BrokerTopicInfo createInstance() throws Exception {
        Path path = Paths.get(System.getProperty("user.dir") + File.separator + BrokerConfig.BROKER_TOPIC_INFO_SAVE_PATH);
        BrokerTopicInfo bean;
        if(Files.exists(path)) {
            log.info("exist broker topic info json file, start with it");
            try(InputStream is = new FileInputStream(path.toFile())) {
                bean = JSON.parseObject(is, StandardCharsets.UTF_8, BrokerTopicInfo.class);
            }
        }else {
            log.info("can not find broker topic info json file, empty start");
            bean = new BrokerTopicInfo();
            bean.setActiveTopicsMap(new ConcurrentHashMap<>());
        }
        return bean;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
