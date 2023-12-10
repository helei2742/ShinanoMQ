package cn.com.shinano.ShinanoMQ.consumer;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@ComponentScan(value = {"cn.com.shinano.ShinanoMQ.consumer.spring"})
@EnableAspectJAutoProxy
public class ConsumerAutoConfig {
}
