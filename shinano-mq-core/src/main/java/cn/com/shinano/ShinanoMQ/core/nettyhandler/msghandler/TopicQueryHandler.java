package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MsgPropertiesConstants;
import cn.com.shinano.ShinanoMQ.base.dto.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.util.MessageUtil;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.RequestHandler;
import cn.com.shinano.ShinanoMQ.core.manager.TopicQueryManager;
import cn.hutool.core.util.StrUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;

/**
 * 处理查询topic相关的信息
 */
public class TopicQueryHandler implements RequestHandler {

    public TopicQueryHandler(TopicQueryManager topicQueryManager) {
        this.topicQueryManager = topicQueryManager;
    }

    private final TopicQueryManager topicQueryManager;

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel) {
        String topicQueryOpt = MessageUtil.getPropertiesValue(message,
                MsgPropertiesConstants.TOPIC_QUERY_OPT_KEY);
        if(StrUtil.isBlank(topicQueryOpt)) return;

        switch (topicQueryOpt) {
            case TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET://查询offset
                topicQueryManager.queryTopicQueueOffset(message, channel);
                break;
            case TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE://根据offset查消息
                Integer count = MessageUtil.getPropertiesInt(message,
                        MsgPropertiesConstants.QUERY_TOPIC_MESSAGE_COUNT_KEY);

                if(count == null || count == 0) count = TopicQueryConstants.QUERY_TOPIC_MESSAGE_COUNT_LIMIT;
                else count = Math.min(count, TopicQueryConstants.QUERY_TOPIC_MESSAGE_COUNT_LIMIT);

                topicQueryManager.queryTopicQueueOffsetMsg(message, count, channel);
                break;
        }
    }
}
