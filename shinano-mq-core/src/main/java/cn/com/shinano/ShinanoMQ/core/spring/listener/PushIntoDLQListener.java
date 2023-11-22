package cn.com.shinano.ShinanoMQ.core.spring.listener;

import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RetryMessage;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;
import cn.com.shinano.ShinanoMQ.core.manager.PersistentSupport;
import cn.com.shinano.ShinanoMQ.core.spring.event.PushIntoDLQEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;


/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
@Slf4j
@Component
public class PushIntoDLQListener implements ApplicationListener<PushIntoDLQEvent> {
    @Autowired
    private PersistentSupport persistentSupport;

    @Override
    public void onApplicationEvent(PushIntoDLQEvent event) {
        // TODO保存到死信队列
        Message message = (Message) event.getSource();

        PutMessageResult result = persistentSupport.syncPutMessage(message);

        switch (result.getStatus()){
            case PUT_OK:
                break;
            default:
                log.error("save message into DLQ fail");
        }

//        File file = new File(BrokerConfig.BROKER_DLQ_MESSAGE_SAVE_PATH);
//        if(!file.exists()) file.mkdirs();
    }
}
