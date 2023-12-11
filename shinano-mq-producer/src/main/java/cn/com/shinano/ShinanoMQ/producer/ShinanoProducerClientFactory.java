package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.producer.manager.ProducerNameServerManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/12/11
 */
@Slf4j
public class ShinanoProducerClientFactory {

    private static final ShinanoProducerClientFactory INSTANCE = new ShinanoProducerClientFactory();

    private volatile static ProducerNameServerManager producerNameServerManager;

    private volatile static ShinanoProducerClient shinanoProducerClient;

    private static String serviceId;

    private static ClusterHost clientHost;

    private static List<ClusterHost> nameservers;


    public static void init(String serviceId, String clientId, String localAddress, int localPort, List<ClusterHost> nameservers) {
        ShinanoProducerClientFactory.serviceId = serviceId;
        ShinanoProducerClientFactory.clientHost = new ClusterHost(clientId, localAddress, localPort);
        ShinanoProducerClientFactory.nameservers = nameservers;
    }

    public static ProducerNameServerManager getProducerNameServerManager(String brokerServiceId) {
        if (producerNameServerManager == null) {
            synchronized (ShinanoProducerClientFactory.class) {
                if(producerNameServerManager == null) {
                    producerNameServerManager = new ProducerNameServerManager(brokerServiceId, serviceId, clientHost, nameservers);
                }
            }
        }
        return producerNameServerManager;
    }

    public static ShinanoProducerClientFactory getInstance() {
        return INSTANCE;
    }

    public ShinanoProducerClient getShinanoProducerClient(ClusterHost brokerAddress) {
        if (shinanoProducerClient == null) {
            synchronized (ShinanoProducerClientFactory.class) {
                if(shinanoProducerClient == null) {
                    shinanoProducerClient = new ShinanoProducerClient(brokerAddress.getAddress(), brokerAddress.getPort(),
                            clientHost.getAddress(), clientHost.getPort(), clientHost.getClientId());
                    shinanoProducerClient.run();
                }
            }
        }
        return shinanoProducerClient;
    }

    public static void main(String[] args) throws InterruptedException {
        List<ClusterHost> list = new ArrayList<>();
        list.add(new ClusterHost(null, "127.0.0.1", 10001));
        list.add(new ClusterHost(null, "127.0.0.1", 10002));
        list.add(new ClusterHost(null, "127.0.0.1", 10003));
        ShinanoProducerClientFactory.init("shinano-mq-producer", "producer-1",
                "127.0.0.1", 30001, list);

        ProducerNameServerManager producerNameServerManager = ShinanoProducerClientFactory.getProducerNameServerManager("shinano-mq-broker");

        ShinanoProducerClient shinanoProducerClient = ShinanoProducerClientFactory.INSTANCE.getShinanoProducerClient(producerNameServerManager.getBrokerAddress());

        shinanoProducerClient.sendMessage("topic1", "queue1", "test-factory", System.out::println);

        System.out.println("123");
        TimeUnit.SECONDS.sleep(200000);
    }
}
