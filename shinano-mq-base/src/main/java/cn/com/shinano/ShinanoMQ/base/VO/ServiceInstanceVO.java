package cn.com.shinano.ShinanoMQ.base.VO;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RegisteredHost;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ServiceInstanceVO {
    private List<RegisteredHost> instances;
}
