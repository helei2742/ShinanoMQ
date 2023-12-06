package cn.com.shinano.ShinanoMQ.core.dto;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import com.alibaba.fastjson.JSONArray;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author lhe.shinano
 * @date 2023/12/6
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageHeader {
    private int length;
    private byte[] magic;

    public static MessageHeader generateMessageHeader(byte[] bytes) {
        if (bytes.length != BrokerConfig.MESSAGE_HEADER_LENGTH) {
            throw new IllegalArgumentException("header byte length illegal");
        }
        MessageHeader messageHeader = new MessageHeader();
        ByteBuffer header = ByteBuffer.wrap(bytes);
        messageHeader.setLength(header.getInt());
        byte[] magicByte = new byte[BrokerConfig.MESSAGE_HEADER_MAGIC.length];
        header.get(magicByte);
        messageHeader.setMagic(magicByte);

        return messageHeader;
    }


    public static byte[] generateMessageHeader(int length) {
        return ByteBuffer.allocate(BrokerConfig.MESSAGE_HEADER_LENGTH).putInt(length).put(BrokerConfig.MESSAGE_HEADER_MAGIC).array();
    }
}
