package cn.com.shinano.nameserver.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ClusterHost implements Comparable<ClusterHost>{
    private String clientId;
    private String host;
    private Integer port;

    @Override
    public int compareTo(ClusterHost o) {
        return clientId.compareTo(o.getClientId());
    }
}
