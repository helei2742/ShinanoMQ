package cn.com.shinano.ShinanoMQ.core.spring.factorybean;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.manager.client.BrokerConsumerInfo;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
@Slf4j
@Component
public class BrokerConsumerInfoFactoryBean extends AbstractFactoryBean<BrokerConsumerInfo> {
    @Override
    public Class<?> getObjectType() {
        return BrokerConsumerInfo.class;
    }

    @Override
    protected BrokerConsumerInfo createInstance() throws Exception {
        Path path = Paths.get(System.getProperty("user.dir") + File.separator + BrokerConfig.BROKER_CONSUMER_INFO_SAVE_PATH);
        BrokerConsumerInfo bean;
        if(Files.exists(path)) {
            log.info("exist broker consumer info json file, start with it");
            try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path.toFile()))) {
                StringBuilder sb = new StringBuilder();
                byte[] bytes = new byte[1024];
                int len = 0;
                while ((len=bis.read(bytes))!=-1) {
                    sb.append(new String(bytes, 0, len, StandardCharsets.UTF_8));
                }
                bean = JSON.parseObject(sb.toString(), BrokerConsumerInfo.class);
            }
        }else {
            log.info("can not find broker consumer info json file, empty start");
            bean = new BrokerConsumerInfo();
        }
        return bean;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
