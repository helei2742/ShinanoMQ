package cn.com.shinano.ShinanoMQ.core.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.charset.StandardCharsets;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IndexNode implements Comparable<IndexNode>{
    private Long logicOffset;
    private Integer fileOffset;
    public byte[] toSaveBytes() {
        return toSaveString().getBytes(StandardCharsets.UTF_8);
    }
    public String toSaveString() {
        return "<" + logicOffset + "-" + fileOffset + ">\n";
    }
    public static IndexNode toIndexNode(String line) {
        String[] split = line.replace("<", "").replace(">", "").split("-");
        IndexNode indexNode = new IndexNode();
        indexNode.setLogicOffset(Long.parseLong(split[0]));
        indexNode.setFileOffset(Integer.parseInt(split[1]));
        return indexNode;
    }

    @Override
    public int compareTo(IndexNode o) {
        return this.logicOffset.compareTo(o.getLogicOffset());
    }
}
