package cn.com.shinano.ShinanoMQ.base.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
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
    public static final String HOST_TYPE_KEY = "host_type";
    public static final String SLAVE_KEY = "slave";
    public static final String MASTER_KEY = "master";

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
