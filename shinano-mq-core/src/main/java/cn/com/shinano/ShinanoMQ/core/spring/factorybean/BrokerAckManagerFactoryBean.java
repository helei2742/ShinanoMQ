package cn.com.shinano.ShinanoMQ.core.spring.factorybean;

import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.manager.BrokerAckManager;
import cn.com.shinano.ShinanoMQ.core.manager.impl.AsyncBrokerAckManager;
import cn.com.shinano.ShinanoMQ.core.manager.impl.SyncBrokerAckManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AbstractFactoryBean;


import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;

/**
 * @author lhe.shinano
 * @date 2023/11/10
 */
@Slf4j
@Component
public class BrokerAckManagerFactoryBean extends AbstractFactoryBean<BrokerAckManager> {

    @Autowired
    private BrokerSpringConfig springConfig;

    @Override
    public Class<?> getObjectType() {
        return BrokerAckManager.class;
    }

    @Override
    protected BrokerAckManager createInstance() throws Exception {
        BrokerAckManager bean;
        if ("async".equals(springConfig.getProducerCommitAckType())) {
            Integer batchSize = springConfig.getProducerCommitAckBatchSize();
            Long ttl = springConfig.getProducerCommitAckTtl();
            Integer thread = springConfig.getProducerCommitAckThread();

            batchSize = Math.max(batchSize, 200);
            ttl = Math.max(ttl, 200);
            thread = Math.max(thread, 1);

            bean = new AsyncBrokerAckManager(batchSize, ttl, Executors.newFixedThreadPool(thread));

            log.info("ShinanoMq ack type is async, batchSize[{}], ttl [{}], thread count [{}]", batchSize, ttl, thread);
        } else {
            bean = new SyncBrokerAckManager();
            log.info("ShinanoMq ack type is sync");
        }

        return bean;
    }
}
