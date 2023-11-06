package cn.com.shinano.ShinanoMQ.core;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.stereotype.Component;

public class ShinanoMQBrokerFactoryBean extends AbstractFactoryBean<ShinanoMQBroker> {


    @Override
    public Class<?> getObjectType() {
        return null;
    }

    @Override
    protected ShinanoMQBroker createInstance() throws Exception {
        return null;
    }
}
