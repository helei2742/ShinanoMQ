package cn.com.shinano.ShinanoMQ.base.dto;

import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

@Data
public class Message {
    private String topic;
    private String queue;
    private Integer flag;
    private Map<String, String> properties;
    private byte[] body;
    private String transactionId;

    public void release(){}

    public void clear() {
//        if(this.properties != null)
//            this.properties.clear();
//        this.flag = null;
//        this.topic =null;
//        this.queue = null;
//        this.transactionId = null;
//        this.body = null;
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", queue='" + queue + '\'' +
                ", flag=" + flag +
                ", properties=" + properties +
                ", body=" + (body ==null ?"null" :new String(body, StandardCharsets.UTF_8)) +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }
}
