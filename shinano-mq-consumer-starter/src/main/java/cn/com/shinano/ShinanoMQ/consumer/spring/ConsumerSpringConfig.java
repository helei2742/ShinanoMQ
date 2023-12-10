package cn.com.shinano.ShinanoMQ.consumer.spring;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.util.CommonUtil;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Configuration
@ConfigurationProperties(prefix = "shinano.mq.consumer")
public class ConsumerSpringConfig {
    public final static String SHIANNOMQ_BROKER_HOST = "shinanoMQ_broker_host";
    public final static String NAME_SERVER_CLUSTER = "name_server_cluster";
    public final static String CONSUMER_CLIENT_MAP_KEY = "consumer_client_map";

    private String[] nameServer;
    private String serviceId;
    private String brokerAddress;
    private Integer brokerPort;
    private String clientId;

    @Bean(name = SHIANNOMQ_BROKER_HOST)
    @ConditionalOnMissingBean(name = SHIANNOMQ_BROKER_HOST)
    public ClusterHost brokerHost() {
        return new ClusterHost("", brokerAddress, brokerPort);
    }

    @Bean(name = NAME_SERVER_CLUSTER)
    @ConditionalOnMissingBean(name = NAME_SERVER_CLUSTER)
    public List<ClusterHost> nameserverCluster() {
        ArrayList<ClusterHost> list = new ArrayList<>();
        for (String s : nameServer) {
            list.add(CommonUtil.clusterHostGenerate(s));
        }
        return list;
    }

    @Bean(name = CONSUMER_CLIENT_MAP_KEY)
    @ConditionalOnMissingBean(name = CONSUMER_CLIENT_MAP_KEY)
    public Map<ClusterHost, ShinanoConsumerClient> producerClientMap() {
        return new ConcurrentHashMap<>();
    }
}
