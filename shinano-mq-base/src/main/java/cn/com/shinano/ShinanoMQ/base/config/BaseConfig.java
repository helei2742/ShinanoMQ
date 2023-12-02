package cn.com.shinano.ShinanoMQ.base.config;

import cn.hutool.core.lang.Dict;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.setting.yaml.YamlUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;

@Slf4j
public class BaseConfig {
    public static String serviceId;
    static {
        URL url = BaseConfig.class.getClassLoader().getResource("./application.yml");
        if(url != null) {
            try {
                Dict dict = YamlUtil.load(new FileReader(url.getFile()));
                serviceId = dict.getByPath("shinano.mq.broker.serviceId", String.class);
                System.out.println(serviceId+"---");
            } catch (FileNotFoundException e) {
                log.error("load base config error", e);
            }
        }
    }
}
