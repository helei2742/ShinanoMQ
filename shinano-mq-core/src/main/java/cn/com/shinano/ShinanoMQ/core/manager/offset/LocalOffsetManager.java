package cn.com.shinano.ShinanoMQ.core.manager.offset;

import cn.com.shinano.ShinanoMQ.core.manager.topic.BrokerTopicInfo;
import cn.com.shinano.ShinanoMQ.core.manager.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
public class LocalOffsetManager implements OffsetManager {

    private static final ConcurrentMap<String, Long> offsetMap = new ConcurrentHashMap<>();

    @Autowired
    private BrokerTopicInfo brokerTopicInfo;

    /**
     * 更新属性 offsetMap 中的offset
     * @param topic topic name
     * @param queue queue name
     * @param offset 当前queue的offset
     */
    @Override
    public void updateTopicQueueOffset(String topic, String queue, long offset) {
        offsetMap.put(BrokerUtil.makeTopicQueueKey(topic, queue), offset);
        brokerTopicInfo.updateOffset(topic, queue, offset);
    }

    /**
     * 查询topic-queue 当前写入的offset
     * @param topic topic name
     * @param queue queue name
     * @return -1代表没有该queue，大于等于0为当前offset大小
     */
    @Override
    public long queryTopicQueueOffset(String topic, String queue) {
        Long aLong = -1L;
        String key = BrokerUtil.makeTopicQueueKey(topic, queue);
        synchronized (key.intern()) {
            aLong = offsetMap.get(key);
            if(aLong == null) { //没查到，有可能是重启后没新消息，需要看文件是否存在

                Long offset = brokerTopicInfo.queryTopicQueueOffset(topic, queue);
                offsetMap.put(key, offset);
                return offset;
            }
        }
        return aLong;
    }
}
