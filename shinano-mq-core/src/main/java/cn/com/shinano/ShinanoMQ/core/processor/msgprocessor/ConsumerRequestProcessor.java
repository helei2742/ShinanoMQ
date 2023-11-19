package cn.com.shinano.ShinanoMQ.core.processor.msgprocessor;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.com.shinano.ShinanoMQ.core.manager.ConsumeOffsetManager;
import cn.com.shinano.ShinanoMQ.core.processor.RequestProcessor;
import com.alibaba.fastjson.JSONArray;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ConsumerRequestProcessor implements RequestProcessor {

    private final ConsumeOffsetManager consumeOffsetManager;

    public ConsumerRequestProcessor(ConsumeOffsetManager consumeOffsetManager) {
        this.consumeOffsetManager = consumeOffsetManager;
    }

    @Override
    public void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand, Channel channel) {
        String clientId = remotingCommand.getExtFieldsValue(ExtFieldsConstants.CLIENT_ID_KEY);
        String opt = remotingCommand.getExtFieldsValue(ExtFieldsConstants.CONSUMER_OPT_KEY);
        String topic = remotingCommand.getTopic();
        String queue = remotingCommand.getQueue();
        String transactionId = remotingCommand.getTransactionId();
        CompletableFuture<RemotingCommand> future = null;

        switch (opt) {
            case ExtFieldsConstants.CONSUMER_BATCH_ACK:
                Long minACK = remotingCommand.getExtFieldsLong(ExtFieldsConstants.CONSUMER_MIN_ACK_OFFSET_KEY);
                List<Long> offsets = JSONArray.parseArray(new String(remotingCommand.getBody()), Long.class);
                future = consumeOffsetManager.resolveConsumeOffset(clientId, topic, queue, minACK, offsets);
                break;
        }

        RemotingCommand response = null;
        try {
            if(future != null){
                response = future.get();
                response.setTransactionId(transactionId);
                NettyChannelSendSupporter.sendMessage(response, channel);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("query topic queue offset got an error", e);
            response = RemotingCommandPool.getObject();
            response.setTransactionId(transactionId);
            response.setFlag(RemotingCommandFlagConstants.CONSUMER_MESSAGE_RESULT);
            response.addExtField(ExtFieldsConstants.CONSUMER_BATCH_ACK_RESULT, "false");
            response.setCode(RemotingCommandCodeConstants.FAIL);
            NettyChannelSendSupporter.sendMessage(response, channel);
        }
    }
}
