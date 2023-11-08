package cn.com.shinano.ShinanoMQ.core.web.dto;

import lombok.Data;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
@Data
public class TopicRequestDTO {
    private String topic;
    private List<String> queues;
}
