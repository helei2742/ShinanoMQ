package cn.com.shinano.ShinanoMQ.base.VO;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageListVO {
    private List<SaveMessage> messages;
    private Long nextOffset;

    private final static MessageListVO EMPTYVO = new MessageListVO(new ArrayList<>(), -1L);

    public static MessageListVO empty() {
        return EMPTYVO;
    }
}
