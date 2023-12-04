package cn.com.shinano.ShinanoMQ.base;

import cn.com.shinano.ShinanoMQ.base.VO.BatchAckVO;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
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
     * @param remotingCommand
     */
    public void resolveBatchACK(RemotingCommand remotingCommand) {
        BatchAckVO vo = ProtostuffUtils.deserialize(remotingCommand.getBody(), BatchAckVO.class);

        List<String> successTsIdLIst = vo.getSuccessTsIdLIst();
        List<String> failIdLIst = vo.getFailTsIdList();

        if(successTsIdLIst != null) {
            successTsIdLIst.forEach(tsId->invokeSuccessCallBack(tsId, null));
        }

        if(failIdLIst != null) {
            failIdLIst.forEach(tsId->invokeFailCallBack(tsId, null));
        }
    }
}
