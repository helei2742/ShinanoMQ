package cn.com.shinano.ShinanoMQ.producer.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.dto.MsgPropertiesConstants;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgHandler;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ProducerClientInitHandler implements ClientInitMsgHandler {

    @Override
    public void initClient(Map<String, String> prop) {
        if(prop == null) return;

        ProducerConfig.SINGLE_MESSAGE_LENGTH = Integer.parseInt(
                prop.getOrDefault(MsgPropertiesConstants.SINGLE_MESSAGE_LENGTH_KEY,
                String.valueOf(ProducerConfig.SINGLE_MESSAGE_LENGTH)));

        ProducerConfig.QUERY_MESSAGE_MAX_COUNT = Integer.parseInt(
                prop.getOrDefault(MsgPropertiesConstants.SINGLE_MESSAGE_LENGTH_KEY,
                        String.valueOf(ProducerConfig.QUERY_MESSAGE_MAX_COUNT)));

        log.info("init client by config [{}]", prop);
    }
}
