package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.producer.spring.DefaultResultCallback;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface ShinanoProducer {
    String host() default "";
    int port() default -1;
    String topic();
    String queue();

    Class<?> callbackClass() default DefaultResultCallback.class;
}
