package cn.com.shinano.ShinanoMQ.consmer.manager;


import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.support.CommitLocalSupport;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ConsumerOffsetManager {
    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    private final ConcurrentMap<String, List<Long>> consumerOffsetMap = new ConcurrentHashMap<>();

    private final ShinanoConsumerClient consumerClient;

    public ConsumerOffsetManager(ShinanoConsumerClient consumerClient) {
        this.consumerClient = consumerClient;
    }

    /**
     * 向broker提交消费offset
     * @param clientId
     * @param topic
     * @param queue
     * @param offset
     */
    public void commitOffset(String clientId, String topic, String queue, Long offset) {
        String key = getKey(clientId, topic, queue);
        consumerOffsetMap.compute(key, (k,v)->{
            if(v == null)
                v = new ArrayList<>();
            v.add(offset);

            if (v.size() >= ConsumerConfig.CONSUME_ACK_BATCH_SIZE) {
                final List<Long> finalV = v;
                v = new ArrayList<>();
                executor.execute(()->{
                    RemotingCommand command = new RemotingCommand();
                    command.setFlag(RemotingCommandFlagConstants.CONSUMER_MESSAGE);
                    long min = Long.MAX_VALUE;
                    for (Long aLong : finalV) {
                        min = Math.min(min, aLong);
                    }
                    command.addExtField(ExtFieldsConstants.CLIENT_ID_KEY, clientId);
                    command.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
                    command.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
                    command.addExtField(ExtFieldsConstants.CONSUMER_OPT_KEY, ExtFieldsConstants.CONSUMER_BATCH_ACK);
                    command.addExtField(ExtFieldsConstants.CONSUMER_MIN_ACK_OFFSET_KEY, String.valueOf(min));
                    command.setBody(JSON.toJSONBytes(finalV));

                    pushConsumeOffset(command);
                });
            }
            return v;
        });
    }

    private void pushConsumeOffset(RemotingCommand command) {
        consumerClient.sendMsg(command, remotingCommand->{
            log.info("push consume offset result: [{}]", remotingCommand);
        }, remotingCommand -> {
            log.error("push batch consume offset to remote fail");
            CommitLocalSupport.commitLocal(command);
        });
    }


    private String getKey(String clientId, String topic, String queue) {
        return clientId + "@<" + topic + ":" + queue + ">";
    }
}
