package cn.com.shinano.ShinanoMQ.base.dto;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author lhe.shinano
 * @date 2023/11/24
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ServiceRegistryDTO extends ClusterHost {
    private String serviceId;

    private RegistryState registryState;
}
