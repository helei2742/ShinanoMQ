package cn.com.shinano.ShinanoMQ.core.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.omg.CORBA.PUBLIC_MEMBER;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BrokerResult {
    private String transactionId;
    private PutMessageStatus status;

    public BrokerResult setStatus(PutMessageStatus status) {
        this.status = status;
        return this;
    }
}
