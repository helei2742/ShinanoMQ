package cn.com.shinano.nameserverclient.dto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class ServiceRegistryDTO {
    private String serviceId;
    private String clientId;
    private String address;
    private Integer port;
}
