package cn.com.shinano.ShinanoMQ.consmer.processor;

import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;

import java.util.Map;

public class ConsumerClientInitProcessor implements ClientInitMsgProcessor {
    @Override
    public boolean initClient(Map<String, String> prop) {
        return false;
    }
}
