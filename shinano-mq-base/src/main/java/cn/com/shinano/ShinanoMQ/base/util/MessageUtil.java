package cn.com.shinano.ShinanoMQ.base.util;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import java.nio.charset.StandardCharsets;

public class MessageUtil {

    public static byte[] messageTurnBytes(Message message) {
//        return JSONObject.toJSONString(message).getBytes(StandardCharsets.UTF_8);
        return  ProtostuffUtils.serialize(message);
    }

    public static Message bytesTurnMessage(byte[] bytes) {
//        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), Message.class);

        return ProtostuffUtils.deserialize(bytes, Message.class);
    }

    /**
     * 获取链接的客户端ID
     * @return
     */
    //TODO 暂时这样
    public static String getClientId(Message message) {
        return new String(message.getBody(), StandardCharsets.UTF_8);
    }
}
