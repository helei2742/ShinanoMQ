package cn.com.shinano.ShinanoMQ.core.utils;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.Pair;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.protocol.Serializer;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.hutool.core.util.RandomUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class BrokerUtil {

    public static final String KEY_SEPARATOR = "!@!";

    public static String makeTopicQueueKey(String topic, String queue) {
        return topic + KEY_SEPARATOR + queue;
    }

    public static Pair<String, String> getTopicQueueFromKey(String key) {
        String[] split = key.split(KEY_SEPARATOR);
        return new Pair<>(split[0], split[1]);
    }


    public static String getTransactionId(String transactionId) {
        if(transactionId == null) {
            transactionId = RandomUtil.randomString(8);
        }
        return UUID.nameUUIDFromBytes(transactionId.getBytes(StandardCharsets.UTF_8)).toString();
    }


    /**
     * 消息转换为存储的字节
     * @param message
     * @param offset
     * @return
     */
    public static byte[] messageTurnBrokerSaveBytes(Message message, long offset) {
        SaveMessage saveMessage = new SaveMessage();
        saveMessage.setTransactionId(message.getTransactionId());
        saveMessage.setBody(message.getBody());
        saveMessage.setReconsumeTimes(0);
        saveMessage.setTimestamp(System.currentTimeMillis());
        saveMessage.setStoreHost(BrokerConfig.BROKER_HOST);
        saveMessage.setOffset(offset);
        saveMessage.setReconsumeTimes(message.getRetryTimes());
        saveMessage.setProps(message.getProperties());
//        byte[] bytes = JSONObject.toJSONBytes(saveMessage);
        byte[] bytes = Serializer.Algorithm.Protostuff.serialize(saveMessage);
        byte[] length = ByteBuffer.allocate(8).putInt(bytes.length).array();
        byte[] res = new byte[bytes.length + length.length];
        System.arraycopy(length, 0, res, 0, length.length);
        System.arraycopy(bytes, 0, res, length.length, bytes.length);
        return res;
    }

    public static SaveMessage brokerSaveBytesTurnMessage(byte[] msgBytes) {
        return Serializer.Algorithm.Protostuff.deserialize(msgBytes, SaveMessage.class);
    }





    public static String getKey(String clientId, String topic, String queue) {
        return clientId + KEY_SEPARATOR + topic + KEY_SEPARATOR + queue + KEY_SEPARATOR;
    }

    public static String[] getPropFromKey(String key) {
        return key.split(KEY_SEPARATOR);
    }
}
