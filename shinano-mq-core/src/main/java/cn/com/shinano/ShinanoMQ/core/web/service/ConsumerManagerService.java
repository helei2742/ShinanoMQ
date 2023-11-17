package cn.com.shinano.ShinanoMQ.core.web.service;

import cn.com.shinano.ShinanoMQ.core.web.dto.BrokerRequestDTO;
import cn.com.shinano.ShinanoMQ.core.web.dto.Result;

/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
public interface ConsumerManagerService {
    Result addConsumer(BrokerRequestDTO requestDTO);
}
