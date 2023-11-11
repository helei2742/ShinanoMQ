package cn.com.shinano.ShinanoMQ.base.nettyhandler;

import java.util.Map;

public interface ClientInitMsgHandler {

    /**
     * 通过prop的配置，初始化客户端
     * @param prop
     */
    void initClient(Map<String, String> prop);
}
