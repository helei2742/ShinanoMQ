package cn.com.shinano.ShinanoMQ.base.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaveMessage {

    /**
     * 逻辑偏移
     */
    private Long offset;


    /**
     * 长度 TODO broker存消息自定义才方便得到存入的长度
     */
    private Integer length;
    /**
     * 消息事务id
     */
    private String transactionId;
    /**
     * 生成的时间戳
     */
    private Long timestamp;
    /**
     * 存储这条消息的host
     */
    private String storeHost;
    /**
     * 重复消费次数
     */
    private Integer reconsumeTimes;
    /**
     * 消息内容
     */
    private byte[] body;
}
