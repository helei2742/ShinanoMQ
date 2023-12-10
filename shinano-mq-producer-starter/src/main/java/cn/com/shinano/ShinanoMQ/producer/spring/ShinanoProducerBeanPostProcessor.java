package cn.com.shinano.ShinanoMQ.producer.spring;

import cn.com.shinano.ShinanoMQ.producer.ShinanoProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AbstractAdvisingBeanPostProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Slf4j
@Component
public class ShinanoProducerBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {
    private ApplicationContext applicationContext;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        Class<?> aClass = bean.getClass();
        for (Method method : aClass.getMethods()) {
            if (method.isAnnotationPresent(ShinanoProducer.class)) {
                bean = ProducerAutoProxyFactory.autoProxyShinanoProducerAnnotation(bean,
                       method, aClass, applicationContext);
            }
        }
        return bean;
    }

    @Nullable
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
