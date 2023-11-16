package cn.com.shinano.ShinanoMQ.consmer.dto;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueueData {
    private String queueName;
    private long nextOffset;
    private LinkedList<SaveMessage> queue;

    public void appendIntoQueue(List<SaveMessage> messages) {
        queue.addAll(messages);
    }
}
