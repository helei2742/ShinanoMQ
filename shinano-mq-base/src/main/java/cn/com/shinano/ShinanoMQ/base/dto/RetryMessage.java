package cn.com.shinano.ShinanoMQ.base.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RetryMessage {
    private Long offset;
    private Integer length;
    private Integer retryTimes;

    public void addRetryTimes() {
        if(this.retryTimes == null) this.retryTimes = 0;
        this.retryTimes++;
    }
}
