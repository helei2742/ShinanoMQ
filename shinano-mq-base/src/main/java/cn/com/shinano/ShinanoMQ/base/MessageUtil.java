package cn.com.shinano.ShinanoMQ.base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MessageUtil {

    public static byte[] messageTurnBytes(Message message) {
        return JSONObject.toJSONBytes(message);
    }

    public static Message bytesTurnMessage(byte[] bytes) {
        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), Message.class);
    }

    public static byte[] messageTurnBrokerSaveBytes(Message message) {
        byte[] bytes = JSONObject.toJSONBytes(message);
        byte[] length = ByteBuffer.allocate(8).putInt(bytes.length).array();
        byte[] res = new byte[bytes.length + length.length];
        System.arraycopy(length, 0, res, 0, length.length);
        System.arraycopy(bytes, 0, res, length.length, bytes.length);
        return res;
    }


    /**
     * 获取链接的客户端ID
     * @return
     */
    //TODO 暂时这样
    public static String getClientId(Message message) {
        return message.getValue();
    }
}
