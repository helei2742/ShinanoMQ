package cn.com.shinano.ShinanoMQ.consmer.listener;

import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeMessage;


public interface ConsumerOnMsgListener {

    void successHandler(ConsumeMessage message);

    void failHandler(Exception exception);
}
