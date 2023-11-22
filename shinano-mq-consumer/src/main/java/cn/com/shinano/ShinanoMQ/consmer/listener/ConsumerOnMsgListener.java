package cn.com.shinano.ShinanoMQ.consmer.listener;

import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeMessage;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeResultState;


public interface ConsumerOnMsgListener {

    ConsumeResultState successHandler(ConsumeMessage message);

    void failHandler(Exception exception);
}
