package cn.com.shinano.nameserver.processor;

import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;

import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
public class NameServerClientInitProcessor implements ClientInitMsgProcessor {
    @Override
    public boolean initClient(Map<String, String> prop) {
        return false;
    }
}
