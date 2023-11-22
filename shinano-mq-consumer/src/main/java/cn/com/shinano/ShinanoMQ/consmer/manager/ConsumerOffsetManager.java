package cn.com.shinano.ShinanoMQ.consmer.manager;


import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.support.CommitLocalSupport;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ConsumerOffsetManager {

    private static final String OFFSET_MAP_KEY_SEPARATOR = "!@!";

    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    private final ConcurrentMap<String, List<Long>> consumerOffsetMap = new ConcurrentHashMap<>();

    private final ShinanoConsumerClient consumerClient;

    private final CommitLocalSupport commitLocalSupport;
    
    private final Timer timer;

    public ConsumerOffsetManager(ShinanoConsumerClient consumerClient) {
        this.consumerClient = consumerClient;
        this.commitLocalSupport = new CommitLocalSupport(consumerClient);
        
        //定时把offset发给broker
        this.timer = new Timer("sendConsumerOffset");
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                Set<String> keySet = consumerOffsetMap.keySet();
                for (String key : keySet) {
                    consumerOffsetMap.compute(key, (k,v)->{
                        if(v == null) return null;

                        String[] props = getPropFromKey(k);
                        String clientId = props[0];
                        String topic = props[1];
                        String queue = props[2];
                        
                        if(v.size() > 0) {
                            final List<Long> finalV = v;
                            v = new ArrayList<>();
                            addPushConsumeOffsetTask(clientId, topic, queue, finalV);
                        }
                        return v;
                    });
                }
            }
        }, ConsumerConfig.PUSH_CONSUME_OFFSET_INTERVAL, ConsumerConfig.PUSH_CONSUME_OFFSET_INTERVAL);
    }

    /**
     * 添加发送consume offset 的任务
     * @param clientId clientId
     * @param topic topic
     * @param queue queue
     * @param offsets offsets
     */
    private void addPushConsumeOffsetTask(String clientId, String topic, String queue, List<Long> offsets) {
        executor.execute(() -> {
            RemotingCommand command = new RemotingCommand();
            command.setFlag(RemotingCommandFlagConstants.CONSUMER_MESSAGE);

            long min = offsets.get(0);

            command.addExtField(ExtFieldsConstants.CLIENT_ID_KEY, clientId);
            command.addExtField(ExtFieldsConstants.TOPIC_KEY, topic);
            command.addExtField(ExtFieldsConstants.QUEUE_KEY, queue);
            command.addExtField(ExtFieldsConstants.CONSUMER_OPT_KEY, ExtFieldsConstants.CONSUMER_BATCH_ACK);
            command.addExtField(ExtFieldsConstants.CONSUMER_MIN_ACK_OFFSET_KEY, String.valueOf(min));
            command.setBody(JSON.toJSONBytes(offsets));

            pushConsumeOffset(command);
        });
    }

    /**
     * 向broker提交消费offset
     * @param clientId clientId
     * @param topic topic
     * @param queue queue
     * @param offset offset
     */
    public void commitOffset(String clientId, String topic, String queue, Long offset) {
        String key = getKey(clientId, topic, queue);
        consumerOffsetMap.compute(key, (k,v)->{
            if(v == null)
                v = new ArrayList<>();
            v.add(offset);

            if (v.size() >= ConsumerConfig.CONSUME_ACK_BATCH_SIZE) { //达到数量限制，发送consume offset
                final List<Long> finalV = v;
                v = new ArrayList<>();
                addPushConsumeOffsetTask(clientId, topic, queue, finalV);
            }
            return v;
        });
    }

    /**
     * 提交consume offset 到broker
     * @param command command
     */
    private void pushConsumeOffset(RemotingCommand command) {
        log.debug("push consume offset, [{}]", command);
        consumerClient.sendMsg(command, remotingCommand->{
            log.info("push consume offset result: [{}]", remotingCommand);

        }, remotingCommand -> {
            //发ack失败，提交到本地尝试重试，一直失败最终落盘
            log.error("push batch consume offset to remote fail");
            commitLocalSupport.commitLocal(command);
        });
    }

    private String getKey(String clientId, String topic, String queue) {
        return clientId + OFFSET_MAP_KEY_SEPARATOR + topic + OFFSET_MAP_KEY_SEPARATOR + queue + OFFSET_MAP_KEY_SEPARATOR;
    }
    
    private String[] getPropFromKey(String key) {
        return key.split(OFFSET_MAP_KEY_SEPARATOR);
    }
}
