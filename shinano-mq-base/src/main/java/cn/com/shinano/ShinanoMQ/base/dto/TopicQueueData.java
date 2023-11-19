package cn.com.shinano.ShinanoMQ.base.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicQueueData {

    private List<Pair<String, Long>> queueInfoList;


    public void addQueueInfo(Pair<String, Long> pair) {
        if(queueInfoList == null) queueInfoList = new ArrayList<>();
        for (Pair<String, Long> longPair : queueInfoList) {
            if(longPair.getKey().equals(pair.getKey())) {
                return;
            }
        }
        queueInfoList.add(pair);
    }
}
