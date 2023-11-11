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
        if(this.properties != null)
            this.properties.clear();
        this.flag = null;
        this.topic =null;
        this.queue = null;
        this.transactionId = null;
        this.body = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return Objects.equals(topic, message.topic) && Objects.equals(queue, message.queue) && Objects.equals(flag, message.flag) && Objects.equals(properties, message.properties) && Arrays.equals(body, message.body) && Objects.equals(transactionId, message.transactionId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, queue, flag, properties, transactionId);
        result = 31 * result + Arrays.hashCode(body);
        return result;
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
