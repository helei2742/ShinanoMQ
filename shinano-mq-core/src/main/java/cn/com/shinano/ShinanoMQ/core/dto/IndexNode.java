package cn.com.shinano.ShinanoMQ.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.charset.StandardCharsets;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IndexNode implements Comparable<IndexNode>{
    private Long logicOffset;
    private Long fileOffset;
    public byte[] toSaveBytes() {
        String s = "<" + logicOffset + "-" + fileOffset + ">\n";
        return s.getBytes(StandardCharsets.UTF_8);
    }
    public static IndexNode toIndexNode(String line) {
        String[] split = line.replace("<", "").replace(">", "").split("-");
        IndexNode indexNode = new IndexNode();
        indexNode.setLogicOffset(Long.parseLong(split[0]));
        indexNode.setFileOffset(Long.parseLong(split[1]));
        return indexNode;
    }

    @Override
    public int compareTo(IndexNode o) {
        return this.logicOffset.compareTo(o.getLogicOffset());
    }
}
