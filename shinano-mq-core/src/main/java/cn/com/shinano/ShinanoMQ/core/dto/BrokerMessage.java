package cn.com.shinano.ShinanoMQ.core.dto;

import cn.com.shinano.ShinanoMQ.base.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BrokerMessage {
    private Long id;
    private Message message;
}
