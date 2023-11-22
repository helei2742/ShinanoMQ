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
    private boolean isSubmit = false;
    @Override
    public boolean equals(Object o) {
        if (o instanceof OffsetBufferEntity) {
            OffsetBufferEntity e = (OffsetBufferEntity) o;
            return e.offset.equals(this.offset);
        }
        return false;
    }
}
