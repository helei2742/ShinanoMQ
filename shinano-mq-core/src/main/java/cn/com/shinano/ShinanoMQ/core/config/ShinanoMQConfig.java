package cn.com.shinano.ShinanoMQ.core.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "shinano.mq.broker")
public class ShinanoMQConfig {

    /**
     * 端口
     */
    private Integer port;

    /**
     * broker持久化的路口
     */
    private String persistentFileLocation;

    /**
     * broker单个持久化文件的大小
     */
    private Long persistentFileSize;

    /**
     * broker向producer提交ack的批大小
     */
    private Integer producerCommitAckBatchSize;

    /**
     * broker向producer提交ack的最小等待时间
     */
    private Long producerCommitAckTtl;
}
