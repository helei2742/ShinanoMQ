package cn.com.shinano.ShinanoMQ.core.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BrokerResult {
    private String transactionId;
    private Boolean success;
}
