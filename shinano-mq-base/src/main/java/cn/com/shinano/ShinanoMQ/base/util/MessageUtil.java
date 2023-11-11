package cn.com.shinano.ShinanoMQ.base.util;

import cn.com.shinano.ShinanoMQ.base.dto.Message;


import java.util.HashMap;

public class MessageUtil {

    public static byte[] messageTurnBytes(Message message) {
//        return JSONObject.toJSONString(message).getBytes(StandardCharsets.UTF_8);
        return  ProtostuffUtils.serialize(message);
    }

    public static Message bytesTurnMessage(byte[] bytes) {
//        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), Message.class);

        return ProtostuffUtils.deserialize(bytes, Message.class);
    }

    public static String getPropertiesValue(Message message, String key) {
        if(message.getProperties() == null) return "";
        return message.getProperties().getOrDefault(key, "");
    }

    public static Integer getPropertiesInt(Message message, String key) {
        String value = getPropertiesValue(message, key);
        if(value == null || value.equals("")) return null;

        return Integer.parseInt(value);
    }

    public static void setPropertiesValue(Message message, String ...kvArray) {
        if (message.getProperties() == null) {
            message.setProperties(new HashMap<>());
        }
        for (int i = 0; i + 1 < kvArray.length; i+=2) {
            message.getProperties().put(kvArray[i], kvArray[i+1]);
        }
    }
}
