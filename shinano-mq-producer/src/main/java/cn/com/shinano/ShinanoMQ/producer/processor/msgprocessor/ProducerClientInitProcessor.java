package cn.com.shinano.ShinanoMQ.producer.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ProducerClientInitProcessor implements ClientInitMsgProcessor {

    @Override
    public boolean initClient(Map<String, String> prop) {
        if(prop == null) return false;

        ProducerConfig.SINGLE_MESSAGE_LENGTH = Integer.parseInt(
                prop.getOrDefault(ExtFieldsConstants.SINGLE_MESSAGE_LENGTH_KEY,
                String.valueOf(ProducerConfig.SINGLE_MESSAGE_LENGTH)));

        ProducerConfig.QUERY_MESSAGE_MAX_COUNT = Integer.parseInt(
                prop.getOrDefault(ExtFieldsConstants.SINGLE_MESSAGE_LENGTH_KEY,
                        String.valueOf(ProducerConfig.QUERY_MESSAGE_MAX_COUNT)));

        log.info("init client by config [{}]", prop);

        return true;
    }
}
