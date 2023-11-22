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

    private List<QueueInfo> queueInfoList;


    public void addQueueInfo(QueueInfo qi) {
        if(queueInfoList == null) queueInfoList = new ArrayList<>();
        for (QueueInfo queueInfo : queueInfoList) {
            if(qi.getQueue().equals(queueInfo.getQueue())) {
                return;
            }
        }
        queueInfoList.add(qi);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class QueueInfo{
        private String queue;
        private Long offset;
        private Integer consumeIndex;

        public void addConsumeIndex() {
            if(consumeIndex == null) consumeIndex = 0;
            consumeIndex++;
        }
    }
}
