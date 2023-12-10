package cn.com.shinano.ShinanoMQ.producer;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@ComponentScan(value = {"cn.com.shinano.ShinanoMQ.producer.spring"})
@EnableAspectJAutoProxy
public class ProducerAutoConfig {
}
