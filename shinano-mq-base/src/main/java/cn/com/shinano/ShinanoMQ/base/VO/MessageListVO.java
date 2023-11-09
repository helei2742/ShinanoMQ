package cn.com.shinano.ShinanoMQ.base.VO;

import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
    private Integer nextOffset;
}
