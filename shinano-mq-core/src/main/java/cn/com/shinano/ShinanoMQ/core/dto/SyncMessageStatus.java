package cn.com.shinano.ShinanoMQ.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lhe.shinano
 * @date 2023/12/1
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SyncMessageStatus {
    private long startOffset;
    private long length;
}
