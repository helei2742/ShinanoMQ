package cn.com.shinano.ShinanoMQ.producer.spring;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.util.CommonUtil;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
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
@ConfigurationProperties(prefix = "shinano.mq.producer")
public class ProducerSpringConfig {

    public final static String PRODUCER_CLIENT_MAP_KEY = "producer_client_map";
    public final static String SHIANNOMQ_BROKER_HOST = "shinanoMQ_broker_host";
    public final static String NAME_SERVER_CLUSTER = "name_server_cluster";

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

    @Bean(name = PRODUCER_CLIENT_MAP_KEY)
    @ConditionalOnMissingBean(name = PRODUCER_CLIENT_MAP_KEY)
    public Map<ClusterHost, ShinanoProducerClient> producerClientMap() {
        return new ConcurrentHashMap<>();
    }
}
