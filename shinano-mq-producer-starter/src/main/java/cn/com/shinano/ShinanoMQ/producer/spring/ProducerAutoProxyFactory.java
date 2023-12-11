package cn.com.shinano.ShinanoMQ.producer.spring;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.producer.ProducerResultCallback;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducer;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClientFactory;
import cn.com.shinano.ShinanoMQ.producer.manager.ProducerNameServerManager;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ProducerAutoProxyFactory {

    public static Object autoProxyShinanoProducerAnnotation(Object bean, Method method, Class<?> aClass, ApplicationContext applicationContext) {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTargetClass(aClass);
        proxyFactory.setTarget(bean);
        proxyFactory.addAdvice(new ShinanoProducerAnnotationProxy(applicationContext, method));
        return proxyFactory.getProxy();
    }


    static class ShinanoProducerAnnotationProxy implements MethodInterceptor {
        private final Map<ClusterHost, ShinanoProducerClient> clientMap;
        private final Map<Class<?>, ProducerResultCallback> callbackMap;
        private final ProducerSpringConfig producerSpringConfig;

        private final Method method;
        private final List<ClusterHost> nameservers;
        private final ShinanoProducerClientFactory clientFactory;
        private final ProducerNameServerManager producerNameServerManager;

        public ShinanoProducerAnnotationProxy(ApplicationContext applicationContext, Method method) {

            this.producerSpringConfig = applicationContext.getBean(ProducerSpringConfig.class);
            this.clientMap = applicationContext.getBean(ProducerSpringConfig.PRODUCER_CLIENT_MAP_KEY, Map.class);
            this.nameservers = applicationContext.getBean(ProducerSpringConfig.NAME_SERVER_CLUSTER, List.class);
            this.clientFactory = applicationContext.getBean(ProducerSpringConfig.SHINANO_PRODUCER_CLIENT_FACTORY, ShinanoProducerClientFactory.class);
            this.producerNameServerManager = ShinanoProducerClientFactory.getProducerNameServerManager(producerSpringConfig.getBrokerServiceId());

            this.callbackMap = new ConcurrentHashMap<>();
            this.callbackMap.put(DefaultResultCallback.class, new DefaultResultCallback());
            this.callbackMap.put(ProducerResultCallback.class, new DefaultResultCallback());
            this.method = method;
        }

        @Override
        public Object invoke(MethodInvocation invocation) throws Throwable {
            Object proceed = invocation.proceed();
            Method method = invocation.getMethod();

            if (!this.method.equals(method)) return proceed;

            ShinanoProducer annotation = method.getAnnotation(ShinanoProducer.class);

            if (annotation == null) {
                log.error("method {} didn't have annotation ShinanoProducer.class", method.getName());
                System.exit(-1);
            } else {
                String host = annotation.host();
                int port = annotation.port();
                String topic = annotation.topic();
                String queue = annotation.queue();

                Class<?> callbackClass = annotation.callbackClass();

                ProducerResultCallback callback = callbackMap.compute(callbackClass, (k, v) -> {
                    if (v == null) {
                        try {
                            v = (ProducerResultCallback) callbackClass.getConstructor().newInstance();
                        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                            log.error("create ProducerResultCallback instance fail, please check you config", e);
                            System.exit(-1);
                        }
                    }
                    return v;
                });

                ClusterHost brokerAddress;
                if (nameservers == null || nameservers.size() <= 0) { //没有配置nameserver时，根据host port直接连
                    if (StrUtil.isBlank(host)) {
                        host = producerSpringConfig.getBrokerAddress();
                    }
                    if (port <= 0) {
                        port = producerSpringConfig.getBrokerPort();
                    }
                    brokerAddress = new ClusterHost(null, host, port);
                } else {
                    ClusterHost res = producerNameServerManager.getBrokerAddress();
                    brokerAddress = res==null?new ClusterHost(null, host, port):res;
                }


                ClusterHost remote = new ClusterHost("", host, port);

                ShinanoProducerClient producerClient = clientMap.compute(remote, (k, v) -> {
                    if (v == null) {
                        v = clientFactory.getShinanoProducerClient(brokerAddress);
                    }
                    return v;
                });

                producerClient.sendMessage(topic, queue, proceed, callback::success, callback::fail);
            }
            return proceed;
        }
    }
}
