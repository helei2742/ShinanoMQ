package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.core.manager.TopicQueryManager;
import cn.com.shinano.ShinanoMQ.core.manager.cluster.BrokerClusterTopicOffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.cluster.MessageInstanceSyncSupport;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;


/**
 * @author lhe.shinano
 * @date 2023/12/1
 */
@Slf4j
public class ClusterSyncProcessor implements RequestProcessor {

    private final TopicQueryManager topicQueryManager;

    private final MessageInstanceSyncSupport messageInstanceSyncSupport;

    private final BrokerClusterTopicOffsetManager clusterTopicOffsetManager;

    public ClusterSyncProcessor(TopicQueryManager topicQueryManager,
                                MessageInstanceSyncSupport messageInstanceSyncSupport,
                                BrokerClusterTopicOffsetManager clusterTopicOffsetManager) {
        this.topicQueryManager = topicQueryManager;
        this.messageInstanceSyncSupport = messageInstanceSyncSupport;
        this.clusterTopicOffsetManager = clusterTopicOffsetManager;
    }

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand request, Channel channel) {
        RemotingCommand response = null;
        switch (request.getFlag()) {
            case RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE:
                messageInstanceSyncSupport.saveSyncMsgInSlaveLocal(request, channel);
                break;
            case RemotingCommandFlagConstants.BROKER_SYNC_PULL_MESSAGE:
                Long offset = request.getExtFieldsLong(ExtFieldsConstants.OFFSET_KEY);
                if (offset == null) {
                    channel.writeAndFlush(RemotingCommand.PARAMS_ERROR);
                    return;
                }
                topicQueryManager.queryTopicQueueBytesAfterOffset(request.getTopic(), request.getQueue(), offset, request.getTransactionId(), channel);
                break;
            case RemotingCommandFlagConstants.BROKER_SLAVE_COMMIT_TOPIC_INFO:

                messageInstanceSyncSupport.commitSlaveTopicInfoAndSendNeedSyncMsg(request, channel);
                break;
            default:
                break;
        }

    }
}
