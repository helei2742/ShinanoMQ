package cn.com.shinano.ShinanoMQ.consumer;


import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface ShinanoConsumer {
    String host() default "";
    int port() default -1;
    String topic();
    String queue();

}
