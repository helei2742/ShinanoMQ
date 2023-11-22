package cn.com.shinano.ShinanoMQ.core.manager.topic;

import cn.com.shinano.ShinanoMQ.core.dto.OffsetAndCount;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicInfo {
    private String topic;
    private Map<String, OffsetAndCount> queueInfo;

    public TopicInfo(String topic) {
        this.topic = topic;
        this.queueInfo = new ConcurrentHashMap<>();
    }

    public Long getOffset(String queue) {
        if(!queueInfo.containsKey(queue)) return -1L;
        return queueInfo.get(queue).getOffset();
    }

    public synchronized void setOffset(String queue, long offset) {
        queueInfo.computeIfPresent(queue, (k, v) -> {
            v.setOffset(Math.max(v.getOffset(), offset));
            v.setCount(v.getCount()+1);
            return  v;
        });
    }

    public List<String> removeQueues(List<String> queues) {
        List<String> res = new ArrayList<>();
        for (String queue : queues) {
            queueInfo.compute(queue, (k, v) -> {
                if (v != null) {
                    res.add(queue);
                    return null;
                }
                return v;
            });
        }
        return res;
    }

}
