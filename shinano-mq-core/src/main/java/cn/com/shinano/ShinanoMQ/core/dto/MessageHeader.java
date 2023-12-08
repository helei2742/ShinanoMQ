package cn.com.shinano.ShinanoMQ.core.dto;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

/**
 * @author lhe.shinano
 * @date 2023/12/6
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageHeader {
    //消息长度
    private int length;
    //消息魔数
    private byte[] magic;
    //在数据文件的offset
    private long logOffset;
    //逻辑offset
    private long logicOffset;

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

        messageHeader.setLogOffset(header.getLong());
        messageHeader.setLogicOffset(header.getLong());
        return messageHeader;
    }


    public static byte[] generateMessageHeader(int length) {
        return ByteBuffer.allocate(BrokerConfig.MESSAGE_HEADER_LENGTH)
                .putInt(length)
                .put(BrokerConfig.MESSAGE_HEADER_MAGIC)
                .array();
    }
}
