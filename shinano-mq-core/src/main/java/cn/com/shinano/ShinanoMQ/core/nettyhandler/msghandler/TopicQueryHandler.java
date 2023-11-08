package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.TopicQueryOPT;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.MessageHandler;
import cn.com.shinano.ShinanoMQ.core.service.TopicQueryService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;

/**
 * 处理查询topic相关的质量信息
 */
public class TopicQueryHandler implements MessageHandler {

    public TopicQueryHandler(TopicQueryService topicQueryService) {
        this.topicQueryService = topicQueryService;
    }

    private final TopicQueryService topicQueryService;

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, Message message, Channel channel) {
        Map<String, String> properties = message.getProperties();

        if(properties == null || !properties.containsKey(TopicQueryOPT.TOPIC_QUERY_OPT_KEY)) return;

        switch (properties.get(TopicQueryOPT.TOPIC_QUERY_OPT_KEY)) {
            case TopicQueryOPT.QUERY_TOPIC_QUEUE_OFFSET://查询offset
                topicQueryService.queryTopicQueueOffset(message, channel);
                break;
            case TopicQueryOPT.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE://根据offset查消息
                topicQueryService.queryTopicQueueOffsetMsg(message, channel);
                break;
        }
    }
}
