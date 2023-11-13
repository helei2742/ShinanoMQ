package cn.com.shinano.ShinanoMQ.base.nettyhandler;

import java.util.Map;

public interface ClientInitMsgProcessor {

    /**
     * 通过prop的配置，初始化客户端
     * @param prop
     */
    boolean initClient(Map<String, String> prop);
}
