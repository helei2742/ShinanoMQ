package cn.com.shinano.nameserver;

import cn.com.shinano.ShinanoMQ.base.RemotingCommandDecoder;
import cn.com.shinano.ShinanoMQ.base.RemotingCommandEncoder;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.nameserver.util.TimeWheelUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


import java.net.ConnectException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;


/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
@Slf4j
public class NameServerServiceConnector {

    public static Bootstrap connector;

    public static Map<ClusterHost, ConnectEntry> channelMap = new ConcurrentHashMap<>();

    public static final ExecutorService executor = Executors.newFixedThreadPool(1);

    public static final Set<ClusterHost> needConnectSet = new HashSet<>();

    static {
        connector = new Bootstrap()
            .group(new NioEventLoopGroup())
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<NioSocketChannel>() {
                @Override//链接建立后被调用，进行初始化
                protected void initChannel(NioSocketChannel ch) throws Exception {
                ch.pipeline().addLast(new IdleStateHandler(NameServerConfig.SERVICE_HEART_BEAT_TTL, 0, 0));

                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(NameServerConfig.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                ch.pipeline().addLast(new LengthFieldPrepender(4));

                ch.pipeline().addLast(new RemotingCommandEncoder());
                ch.pipeline().addLast(new RemotingCommandDecoder());

                ch.pipeline().addLast(new AbstractNettyProcessorAdaptor() {

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        sendPingMsg(ctx);
                    }

                    @Override
                    protected void handlerMessage(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
                        ClusterHost host = ctx.channel().attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).get();
                        log.debug("get service message [{}]", remotingCommand);
                        if (remotingCommand.getFlag().equals(RemotingCommandFlagConstants.BROKER_PING)) {
                            channelMap.computeIfPresent(host, (k, v)->{
                                v.usable = true;
                                v.lastPongTimeStamp = System.currentTimeMillis();
                                return v;
                            });
                        }
                    }

                    @Override
                    public void printLog(String logStr) {
                        log.info(logStr);
                    }

                    @Override
                    public void sendPingMsg(ChannelHandlerContext context) {
                        super.sendPingMsg(context);
                        ClusterHost host = context.channel().attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).get();

                        //延时，检查是否收到pong
                        TimeWheelUtil.newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                channelMap.computeIfPresent(host, (k,v)->{
                                    if (System.currentTimeMillis() - v.getLastPongTimeStamp() >= NameServerConfig.SERVICE_HEART_BEAT_TTL*1000) {
                                        log.warn("host [{}] long time no pong since [{}] set shut down it", host, v.getLastPongTimeStamp());
                                        v.usable = false;
                                        v.channel.close();
                                    };
                                    return v;
                                });
                            }
                        }, NameServerConfig.SERVICE_HEART_BEAT_TTL + 10, TimeUnit.SECONDS);
                    }
                });
                }
            });
    }

    public static CompletableFuture<Channel> tryConnectService(ClusterHost host) {
        needConnectSet.add(host);

        return CompletableFuture.supplyAsync(()->{
            if(!channelMap.containsKey(host)) {
                ChannelFuture channelFuture = connector.connect(host.getAddress(), host.getPort());
                channelFuture.addListener(f -> {
                    if (!f.isSuccess() && f.cause() instanceof ConnectException) {
                        log.warn("connect to remote [{}] fail", host);
                    } else {
                        Channel channel = channelFuture.sync().channel();
                        channel.attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).set(host);
                        channelMap.put(host, new ConnectEntry(channel, false, -1));

                        log.info("success connect to service host [{}]", host);
                    }
                });
            }
            return null;
        }, executor);
    }



    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class ConnectEntry {
        private Channel channel;
        private boolean usable;
        private long lastPongTimeStamp;
    }
}
