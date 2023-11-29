package cn.com.shinano.ShinanoMQ.core;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;
import java.util.Arrays;

@SpringBootApplication
@EnableScheduling
public class ShinanoBrokerApplication {
    public static void main(String[] args) {
        System.out.println(Arrays.toString(args));
        String baseDir = System.getProperty("user.dir");

        if (args.length > 0) {
            String[] split = args[0].split("=");
            if (split[0].equals("--spring.profiles.active")) {
                String serviceName = split[1];
                baseDir = System.getProperty("user.dir") + File.separator + serviceName;
            }
        }

        File file = new File(baseDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        BrokerConfig.PERSISTENT_FILE_LOCATION = baseDir + File.separator + "datalog";
        BrokerConfig.BROKER_TOPIC_INFO_SAVE_PATH = baseDir+File.separator+"BrokerTopicInfo.json";
        BrokerConfig.BROKER_CONSUMER_INFO_SAVE_PATH = baseDir+File.separator+"BrokerConsumerInfo.json";
        BrokerConfig.RETRY_LOG_SAVE_DIR = baseDir + File.separator + "retrylog";

        ConfigurableApplicationContext applicationContext
                = SpringApplication.run(ShinanoBrokerApplication.class, args);
    }
}
