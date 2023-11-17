package cn.com.shinano.ShinanoMQ.base.supporter;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;


/**
 * @author lhe.shinano
 * @date 2023/11/16
 */
@Slf4j
public class NettyChannelSendSupporter {


    public static void sendMessage(RemotingCommand remotingCommand, Channel channel) {
        channel.writeAndFlush(remotingCommand);
        log.debug("send remoting command [{}]", remotingCommand);
    }
}
