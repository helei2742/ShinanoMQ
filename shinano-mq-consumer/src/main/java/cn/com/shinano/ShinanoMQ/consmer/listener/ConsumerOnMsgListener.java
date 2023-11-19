package cn.com.shinano.ShinanoMQ.consmer.listener;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;


public interface ConsumerOnMsgListener {

    void successHandler(SaveMessage message);

    void failHandler(Exception exception);
}
