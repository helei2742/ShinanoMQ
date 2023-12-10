package cn.com.shinano.ShinanoMQ.consmer.listener;

import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeMessage;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeResultState;

/**
 * 收到消息的监听
 */
public interface ConsumerOnMsgListener {

    /**
     * 成功的监听
     * @param message message
     * @return ConsumeResultState
     */
    ConsumeResultState successHandler(ConsumeMessage message);

    /**
     * 失败的监听
     * @param exception exception
     */
    void failHandler(Exception exception);
}
