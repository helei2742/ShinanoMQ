package cn.com.shinano.ShinanoMQ.core.nettyhandler.msghandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.MessageHandler;
import cn.com.shinano.ShinanoMQ.core.service.TopicQueryService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

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
        topicQueryService.queryTopicQueueOffsetMsg(message, channel);
    }
}
