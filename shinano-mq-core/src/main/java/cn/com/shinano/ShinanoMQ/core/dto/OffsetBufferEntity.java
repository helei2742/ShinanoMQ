package cn.com.shinano.ShinanoMQ.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OffsetBufferEntity {
    private Long offset;
    private Integer length;
}
