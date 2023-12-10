package cn.com.shinano.ShinanoMQ.consumer.spring;

import cn.com.shinano.ShinanoMQ.consumer.ShinanoConsumer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Component
public class ShinanoConsumerBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {
    private ApplicationContext applicationContext;
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Class<?> aClass = bean.getClass();
        for (Method method : aClass.getMethods()) {
            if (method.isAnnotationPresent(ShinanoConsumer.class)) {
                bean = ConsumerAutoProxyFactory.registryConsumeMethod(bean, aClass, applicationContext, method);
            }
        }
        return bean;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
