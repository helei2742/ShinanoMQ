package cn.com.shinano.ShinanoMQ.core.spring.event;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RetryMessage;
import org.springframework.context.ApplicationEvent;

/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
public class PushIntoDLQEvent extends ApplicationEvent {
    public PushIntoDLQEvent(Message retryMessage) {
        super(retryMessage);
    }
}
