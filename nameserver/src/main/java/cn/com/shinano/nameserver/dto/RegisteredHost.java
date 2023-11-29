package cn.com.shinano.nameserver.dto;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Objects;

/**
 * @author lhe.shinano
 * @date 2023/11/29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RegisteredHost {
    private ClusterHost host;
    private Map<String, String> props;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegisteredHost that = (RegisteredHost) o;
        return Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host);
    }
}
