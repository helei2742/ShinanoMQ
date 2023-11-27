package cn.com.shinano.nameserver;

import cn.com.shinano.ShinanoMQ.base.AbstractNettyClient;
import cn.com.shinano.ShinanoMQ.base.ReceiveMessageProcessor;
import cn.com.shinano.ShinanoMQ.base.RemotingCommandDecoder;
import cn.com.shinano.ShinanoMQ.base.RemotingCommandEncoder;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import cn.com.shinano.nameserver.dto.SendCommandResult;
import cn.com.shinano.nameserver.processor.NameServerClusterProcessorAdaptor;
import cn.com.shinano.nameserver.processor.child.NameServerClusterInitProcessor;
import cn.com.shinano.nameserver.support.MasterManagerSupport;
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
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Slf4j
public class NameServerClusterService extends AbstractNettyClient {
    private String clientId;

    private final NameServerService nameServerService;
    private Bootstrap clientBootstrap;

    private ClusterHost clientHost;

    private ClusterHost connectHost;


    private final AtomicBoolean usable = new AtomicBoolean(false);

    private NameServerClusterProcessorAdaptor processorAdaptor;

    public NameServerClusterService(NameServerService nameServerService, ClusterHost connectHost) {
        super(connectHost.getAddress(), connectHost.getPort());

        this.connectHost = connectHost;
        this.nameServerService = nameServerService;
        this.clientId = nameServerService.getClientId();

        this.clientHost = new ClusterHost(clientId, nameServerService.getHost(), nameServerService.getPort());
        init();
    }


    public void init() {
        NettyClientEventHandler eventHandler = new NettyClientEventHandler() {
            @Override
            public void activeHandler(ChannelHandlerContext ctx) {
                ctx.channel().attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).set(connectHost);

                switch (nameServerService.getState()) {
                    case VOTE:
                    case JUST_START:
                        RemotingCommand command = MasterManagerSupport.voteRemoteMessageBuilder(nameServerService.getStartTime(), clientHost);
                        log.info("client [{}] send vote message [{}] to [{}]", clientHost, command, ctx.channel().remoteAddress());
                        ctx.channel().writeAndFlush(command);
                        break;
                    case RUNNING:
                        command = MasterManagerSupport.setMasterRemoteMessageBuilder(nameServerService.getMaster());
                        log.info("client [{}] send set master message [{}] to [{}]", clientHost, command, ctx.channel().remoteAddress());
                        ctx.channel().writeAndFlush(command);
                        break;
                    case SHUT_DOWN:
                        break;
                }
            }

            @Override
            public void closeHandler(Channel channel) {
                ClusterHost offLineHost = channel.attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).get();
                if (offLineHost != null) {
                    log.info("close [{}]", offLineHost);
                    usable.set(false);
                    nameServerService.serverOffLine(offLineHost);
                    MasterManagerSupport.removeVoteInfo(offLineHost);
                }
            }

            @Override
            public void initSuccessHandler(RemotingCommand remotingCommand) {
                log.info("init");
            }

            @Override
            public void initFailHandler() {
                log.info("fail");
            }

            @Override
            public void exceptionHandler(ChannelHandlerContext ctx, Throwable cause) {
                log.error("error", cause);
            }
        };
        processorAdaptor = new NameServerClusterProcessorAdaptor(nameServerService, eventHandler);

        super.init(clientHost.getClientId(),
                NameServerConfig.SERVICE_HEART_BEAT_TTL,
                new ReceiveMessageProcessor(),
                new NameServerClusterInitProcessor(),
                processorAdaptor,
                eventHandler);

    }


    @Override
    public void run() throws InterruptedException {
        try {
            this.clientBootstrap = new Bootstrap()
                    .group(new NioEventLoopGroup())
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<NioSocketChannel>() {
                        @Override//链接建立后被调用，进行初始化
                        protected void initChannel(NioSocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new IdleStateHandler(0, 0, NameServerConfig.SERVICE_HEART_BEAT_TTL));

                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(NameServerConfig.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                            ch.pipeline().addLast(new LengthFieldPrepender(4));

                            ch.pipeline().addLast(new RemotingCommandEncoder());
                            ch.pipeline().addLast(new RemotingCommandDecoder());

                            ch.pipeline().addLast(processorAdaptor);
                        }
                    });

            tryConnectOther(this,0);
        } catch (Exception e) {
            log.error("error", e);
        }
    }


    /**
     * 不断尝试链接集群其它节点
     */
    private void tryConnectOther(NameServerClusterService client, int retry) {
        TimeWheelUtil.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                try {
                    ChannelFuture channelFuture = clientBootstrap
                            .connect(new InetSocketAddress(connectHost.getAddress(), connectHost.getPort()));

                    channelFuture.addListener(f->{
                        if (!f.isSuccess() && f.cause() instanceof ConnectException) {
                            log.warn("[{}] connect to remote [{}] fail, retry [{}]", clientHost, connectHost, retry);
                            if(retry > NameServerConfig.TRY_CONNECT_OTHER_SERVER_MAX_RETRY) {
                                //超过重试次数，
                                return;
                            }
                            tryConnectOther(client,retry + 1);
                        } else {
                            usable.set(true);
                            log.info("[{}] connect to remote [{}] success", clientHost, connectHost);
                            client.channel = channelFuture.sync().channel();
                        }
                    });
                    channelFuture.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 50, TimeUnit.MILLISECONDS);
    }


    public void sendVoteCommand() {
        RemotingCommand command = MasterManagerSupport.voteRemoteMessageBuilder(nameServerService.getStartTime(), clientHost);
        sendCommand(command);
    }

    public SendCommandResult sendCommand(RemotingCommand command) {
        SendCommandResult result = null;
        if(usable.get()) {
            result = new SendCommandResult();
            sendMsg(command, result::setResult, result::setResult);
        } else {
            log.warn("client [{}] send vote message to [{}] fail, channel closed",
                    clientHost, channel.attr(NameServerConfig.NETTY_CHANNEL_CLIENT_ID_KEY).get());
            return null;
        }
        return result;
    }
}
