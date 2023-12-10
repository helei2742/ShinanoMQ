package cn.com.shinano.ShinanoMQ.consumer.spring;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeMessage;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeResultState;
import cn.com.shinano.ShinanoMQ.consmer.listener.ConsumerOnMsgListener;
import cn.com.shinano.ShinanoMQ.consumer.ShinanoConsumer;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;


@Slf4j
public class ConsumerAutoProxyFactory {
    public static Object registryConsumeMethod(Object bean,
                                               Class<?> aClass,
                                               ApplicationContext applicationContext,
                                               Method method) {
        if (method.getParameterCount() < 1) {
            log.error("shinano consumer registry method params count must bigger than 1");
            return bean;
        }

        ShinanoConsumer anno = method.getAnnotation(ShinanoConsumer.class);

        String host = anno.host();
        int port = anno.port();
        String topic = anno.topic();
        String queue = anno.queue();

        ConsumerSpringConfig consumerSpringConfig = applicationContext.getBean(ConsumerSpringConfig.class);

        if (StrUtil.isBlank(host)) {
            host = consumerSpringConfig.getBrokerAddress();
        }
        if (port <= 0) {
            port = consumerSpringConfig.getBrokerPort();
        }

        ClusterHost remote = new ClusterHost("", host, port);
        Map<ClusterHost, ShinanoConsumerClient> consumerClientMap
                = applicationContext.getBean(ConsumerSpringConfig.CONSUMER_CLIENT_MAP_KEY, Map.class);
        consumerClientMap.putIfAbsent(remote,
                buildShinanoConsumerClient(host, port, consumerSpringConfig.getClientId()));
        ShinanoConsumerClient client = consumerClientMap.get(remote);

        client.onMessage(topic, queue, new ConsumerOnMsgListener() {
            @Override
            public ConsumeResultState successHandler(ConsumeMessage message) {
                Object result = null;
                try {
                    result = method.invoke(bean, message);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    log.error("invoke consume method [{}] error", method, e);
                    return ConsumeResultState.FAIL;
                }
                if (result instanceof ConsumeResultState) {
                    return (ConsumeResultState) result;
                }else {
                    return ConsumeResultState.SUCCESS;
                }
            }

            @Override
            public void failHandler(Exception e) {
                log.error("invoke consume method [{}] error", method, e);
            }
        });

        return bean;
    }

    private static ShinanoConsumerClient buildShinanoConsumerClient(String host, Integer port, String clientId) {
        try {
            ShinanoConsumerClient client
                    = new ShinanoConsumerClient(host, port, clientId);
            client.run();
            return client;
        } catch (InterruptedException e) {
            log.error("start consumer client [{}:{}@{}] error", host, port, clientId, e);
        }
        return null;
    }
}
