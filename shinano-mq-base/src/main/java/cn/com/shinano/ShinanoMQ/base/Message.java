package cn.com.shinano.ShinanoMQ.base;

import lombok.Data;

@Data
public class Message {
    private Integer opt;
    private String topic;
    private String queue;
    private Object value;
}
