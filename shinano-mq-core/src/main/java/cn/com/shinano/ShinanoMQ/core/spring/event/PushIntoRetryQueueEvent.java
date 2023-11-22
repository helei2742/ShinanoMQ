package cn.com.shinano.ShinanoMQ.core.spring.event;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import org.springframework.context.ApplicationEvent;

/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
public class PushIntoRetryQueueEvent extends ApplicationEvent {
    public PushIntoRetryQueueEvent(Message message) {
        super(message);
    }
}
