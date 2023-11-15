package cn.com.shinano.ShinanoMQ.core.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.function.Consumer;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppendMessageResult {
    // Return code
    private AppendMessageStatus status;
    // Where to start writing
    private long wroteOffset;
    // Write Bytes
    private int wroteBytes;

    private byte[] appendContent;

    // Message ID
    private String msgId;
    // Message storage timestamp
    private long storeTimestamp;
}
