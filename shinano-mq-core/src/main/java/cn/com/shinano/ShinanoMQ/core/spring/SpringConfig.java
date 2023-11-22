package cn.com.shinano.ShinanoMQ.core.spring;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
public class SpringConfig implements ApplicationContextAware {

    /**
     * 异步监听
     * @param applicationContext
     * @throws BeansException
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SimpleApplicationEventMulticaster applicationEventMulticaster =
                applicationContext.getBean(AbstractApplicationContext.APPLICATION_EVENT_MULTICASTER_BEAN_NAME, SimpleApplicationEventMulticaster.class);

        Executor taskExecutor = Executors.newFixedThreadPool(1);

        applicationEventMulticaster.setTaskExecutor(taskExecutor);
    }
}
