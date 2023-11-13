package cn.com.shinano.ShinanoMQ.producer.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.VO.BatchAckVO;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.producer.processor.ResultCallBackInvoker;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/10
 */
@Slf4j
public class ReceiveMessageProcessor extends ResultCallBackInvoker {

    @Override
    public void init() {
        super.init();
    }

    /**
     * 处理批量ack
     * @param msg
     */
    public void resolveBatchACK(Message msg) {
        BatchAckVO vo = ProtostuffUtils.deserialize(msg.getBody(), BatchAckVO.class);

        List<String> successTsIdLIst = vo.getSuccessTsIdLIst();
        List<String> failIdLIst = vo.getFailTsIdList();

        log.debug("get a batch ack message, success [{}], fail[{}]", successTsIdLIst, failIdLIst);

        if(successTsIdLIst != null) {
            successTsIdLIst.forEach(tsId->invokeSuccessCallBack(tsId, null));
        }

        if(failIdLIst != null) {
            failIdLIst.forEach(tsId->invokeSuccessCallBack(tsId, null));
        }
    }
}
