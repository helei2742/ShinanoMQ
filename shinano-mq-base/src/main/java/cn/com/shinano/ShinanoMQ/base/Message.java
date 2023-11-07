package cn.com.shinano.ShinanoMQ.base;

import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

@Data
public class Message {
    private String topic;
    private String queue;
    private Integer flag;
    private Map<String, String> properties;
    private byte[] body;
    private String transactionId;

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", queue='" + queue + '\'' +
                ", flag=" + flag +
                ", properties=" + properties +
                ", body=" + new String(body, StandardCharsets.UTF_8) +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }
}
