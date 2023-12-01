package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.AckStatus;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.MessagePool;
import cn.com.shinano.ShinanoMQ.core.manager.cluster.MessageInstanceSyncSupport;
import cn.com.shinano.ShinanoMQ.core.manager.topic.TopicInfo;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import cn.com.shinano.ShinanoMQ.core.manager.BrokerAckManager;
import cn.com.shinano.ShinanoMQ.core.manager.DispatchMessageService;
import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;


/**
 * 处理生产者发送的需要保存的数据消息
 */
@Slf4j
public class SaveMessageRequestProcessor implements RequestProcessor {

    private final TopicManager topicManager;

    private final DispatchMessageService dispatchMessageService;

    private final BrokerAckManager brokerAckManager;

    private final MessageInstanceSyncSupport messageInstanceSyncSupport;

    public SaveMessageRequestProcessor(TopicManager topicManager,
                                       DispatchMessageService dispatchMessageService,
                                       MessageInstanceSyncSupport messageInstanceSyncSupport,
                                       BrokerAckManager brokerAckManager) {
        this.topicManager = topicManager;
        this.dispatchMessageService = dispatchMessageService;
        this.messageInstanceSyncSupport = messageInstanceSyncSupport;

        this.brokerAckManager = brokerAckManager;
    }

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand, Channel channel) {
        Message message = MessagePool.getObject();
        message.setTransactionId(remotingCommand.getTransactionId());
        message.setTopic(remotingCommand.getTopic());
        message.setQueue(remotingCommand.getQueue());
        message.setBody(remotingCommand.getBody());

        //topic不存在，返回失败
        if (!topicManager.isTopicExist(message.getTopic(), message.getQueue())) {
            brokerAckManager.sendAck(message.getTransactionId(), AckStatus.FAIL.getValue(), channel);
            return;
        }

        if (remotingCommand.getFlag().equals(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE)) {
            TopicInfo topicInfo = topicManager.getTopicInfo(message.getTopic());
            Long offset = topicInfo.getOffset(message.getQueue());
            //当前broker 的 offset 小于 master 的 offset，需要同步
            Long masterOffset = remotingCommand.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);
            if (offset < masterOffset) {
                log.warn("topic[{}]-queue[{}], local offset < master offset, need sync data", message.getTopic(), message.getQueue());
                //TODO, 暂写进一个文件，同时添加从master拉取缺失部分的任务
                messageInstanceSyncSupport.trySyncMsgFromInstance(message, offset, masterOffset, channel);

                return;
            } else if(offset > masterOffset) { //当前offset 大于 master offset， 数据不对，需要删除重新获取
                log.warn("topic[{}]-queue[{}], local offset > master offset, well rebuilt local from master", message.getTopic(), message.getQueue());
                //TODO 停止服务，获取到master数据后再恢复

                return;
            }
        }

        //只有master并且 flag 设置不是 BROKER_ONLY_SAVE_MESSAGE 才向其它broker同步
        boolean isSyncMsgToCluster = (!(remotingCommand.getFlag() == RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE)
                && "master".equals(dispatchMessageService.brokerSpringConfig.getType()));

        //设置该消息的响应ACK状态
//        brokerAckManager.setAckFlag(messageId, channel);
//        交给下游处理
//        dispatchMessageService.addMessageIntoQueue(brokerMessage);
        RemotingCommand response = dispatchMessageService.saveMessage(message, channel, isSyncMsgToCluster);
        if(response != null) {
            channel.writeAndFlush(response);
        }
    }
}
