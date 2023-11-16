package cn.com.shinano.ShinanoMQ.core.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PutMessageResult {
    private String transactionId;
    private PutMessageStatus status;

    public PutMessageResult setStatus(PutMessageStatus status) {
        this.status = status;
        return this;
    }
}
