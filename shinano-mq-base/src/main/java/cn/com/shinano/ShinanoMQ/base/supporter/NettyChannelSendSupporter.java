package cn.com.shinano.ShinanoMQ.base.supporter;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import io.netty.channel.Channel;


/**
 * @author lhe.shinano
 * @date 2023/11/16
 */
public class NettyChannelSendSupporter {


    public static void sendMessage(RemotingCommand remotingCommand, Channel channel) {
        channel.writeAndFlush(remotingCommand);
    }
}
