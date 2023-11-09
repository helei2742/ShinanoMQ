package cn.com.shinano.ShinanoMQ.core.dto;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BrokerMessage {
    private String id;
    private Message message;
}
