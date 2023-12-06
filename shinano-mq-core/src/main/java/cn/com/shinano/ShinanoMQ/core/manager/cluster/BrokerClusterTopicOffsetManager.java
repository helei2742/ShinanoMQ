package cn.com.shinano.ShinanoMQ.core.manager.cluster;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.Pair;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.protocol.Serializer;
import cn.com.shinano.ShinanoMQ.core.dto.OffsetAndCount;
import cn.com.shinano.ShinanoMQ.core.manager.NameServerManager;
import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/12/5
 */
@Slf4j
@Component
public class BrokerClusterTopicOffsetManager {

    private final ConcurrentMap<ClusterHost, SlaveTopicInfo> slaveTopicInfo = new ConcurrentHashMap<>();

    @Autowired
    private TopicManager topicManager;

    @Autowired
    private NameServerManager nameServerManager;

    public CompletableFuture<List<String>> updateSlaveTopicInfo(RemotingCommand request) {
        return CompletableFuture.supplyAsync(()->{
            String json = request.getExtFieldsValue(ExtFieldsConstants.HOST_JSON);
            ClusterHost clusterHost = JSON.parseObject(json, ClusterHost.class);

            List<String> lines = Serializer.Algorithm.JSON.deserializeList(request.getBody(), String.class);
            log.debug("lines [{}]", lines);

            List<String> list = new ArrayList<>();

            for (String line : lines) {
                String[] split = line.split(BrokerUtil.KEY_SEPARATOR);
                String topic = split[0];
                String queue = split[1];
                long offset = Long.parseLong(split[2]);
                int count = Integer.parseInt(split[3]);

                OffsetAndCount offsetAndCount = topicManager.getTopicInfo(topic).getQueueInfo().get(queue);

                if (offset != offsetAndCount.getOffset()) {
                    list.add(topic + BrokerUtil.KEY_SEPARATOR + queue + BrokerUtil.KEY_SEPARATOR + offset
                            + BrokerUtil.KEY_SEPARATOR + offsetAndCount.getOffset() + BrokerUtil.KEY_SEPARATOR + offsetAndCount.getCount());
                }

                updateSlaveTopicInfo(clusterHost, topic, queue, offset, count);
            }
            return list;
        });
    }

    public void updateSlaveTopicInfo(ClusterHost slaveHost, String topic, String queue, long offset, int count) {
        log.debug("update slave topic info host[{}], topic[{}]-queue[{}]-offset[{}]-count[{}]", slaveHost, topic, queue, offset, count);

        String key = BrokerUtil.makeTopicQueueKey(topic, queue);
        slaveTopicInfo.compute(slaveHost, (k, v)->{
            if (v == null) {
               v = new SlaveTopicInfo();
            }
            v.updateOffsetAndCount(key, offset, count);
            return v;
        });
    }

    @Data
    static class SlaveTopicInfo{
        private Map<String, Pair<Integer, Long>> topicQueueMap;
        SlaveTopicInfo() {
            this.topicQueueMap = new HashMap<>();
        }
        public void updateOffsetAndCount(String key, long offset, int count) {
            Pair<Integer, Long> pair = this.topicQueueMap.getOrDefault(key, new Pair<>());
            pair.setKey(count);
            pair.setValue(offset);
            this.topicQueueMap.put(key, pair);
        }
    }
}
