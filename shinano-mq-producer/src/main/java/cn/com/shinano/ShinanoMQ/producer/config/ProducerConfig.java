package cn.com.shinano.ShinanoMQ.producer.config;

import lombok.Data;

@Data
public class ProducerConfig {

    public static final int IDLE_TIME_SECONDS = 30;

    public static final String PRODUCER_CLIENT_ID = "producer-client-1";
}
