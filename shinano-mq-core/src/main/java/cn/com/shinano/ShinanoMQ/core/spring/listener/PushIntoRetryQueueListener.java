package cn.com.shinano.ShinanoMQ.core.spring.listener;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RetryMessage;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageStatus;
import cn.com.shinano.ShinanoMQ.core.manager.PersistentSupport;
import cn.com.shinano.ShinanoMQ.core.spring.event.PushIntoRetryQueueEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
@Slf4j
@Component
public class PushIntoRetryQueueListener implements ApplicationListener<PushIntoRetryQueueEvent> {
    @Autowired
    private PersistentSupport persistentSupport;

    private final ConcurrentMap<String, List<RetryMessage>> retryMap = new ConcurrentHashMap<>();

    @Override
    public void onApplicationEvent(PushIntoRetryQueueEvent event) {
        /*BrokerRetryMessage brokerRetryMessage = (BrokerRetryMessage) event.getSource();
        String key = BrokerUtil.getKey(brokerRetryMessage.getClientId(), brokerRetryMessage.getTopic(), brokerRetryMessage.getQueue());
        RetryMessage retryMessage = brokerRetryMessage.getRetryMessage();

        retryMap.compute(key, (k,v)->{
            if(v == null) {
                v = new ArrayList<>();
                v.add(retryMessage);
                return v;
            }
            for (RetryMessage r : v) {
                if(retryMessage.getOffset().equals(r.getOffset())){
                    r.addRetryTimes();
                    return v;
                }
            }
            v.add(retryMessage);
            return v;
        });*/

        //写到重试缓存中，只记录offset、length、重试次数
//        File file = StoreFileUtil.getRetryLogSaveFile(brokerRetryMessage.getClientId(), brokerRetryMessage.getTopic(), brokerRetryMessage.getQueue());
//        if(file.length() > 0)

        Message message = (Message) event.getSource();
//        long offset = Long.parseLong(message.getProperties().get("last_message_offset"));
//        int length = Integer.parseInt(message.getProperties().get("last_message_length"));
//        byte[] body = persistentSupport.getOffsetMessageBody(offset, length);
//        message.setBody(body);

        CompletableFuture<PutMessageResult> future = persistentSupport.asyncPutMessage(message);
        PutMessageResult result = null;
        try {
            result = future.get();
            if (result.getStatus() == PutMessageStatus.PUT_OK) {
                log.warn("add retry message into queue success {}", message);
            } else {
                log.error("retry append message into queue fail, status[{}], {}", result.getStatus(), message);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("retry append message into queue error, {}", message, e);
        }
    }
}
