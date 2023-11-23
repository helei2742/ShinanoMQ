package cn.com.shinano.nameserver;


import cn.com.shinano.ShinanoMQ.base.RemotingCommandDecoder;
import cn.com.shinano.ShinanoMQ.base.RemotingCommandEncoder;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.nameserver.config.NameServerConfig;
import cn.com.shinano.nameserver.dto.ClusterHost;
import cn.com.shinano.nameserver.dto.NameServerState;
import cn.com.shinano.nameserver.dto.VoteInfo;
import cn.com.shinano.nameserver.processor.NameServerProcessorAdaptor;
import cn.com.shinano.nameserver.support.MasterManagerSupport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Data
@Slf4j
public class NameServerService {
    private final List<ClusterHost> clusterHosts;

    private final String clientId;

    private final String host;

    private final int port;

    private ChannelFuture nameserverChannelFuture;

    private final ConcurrentMap<ClusterHost, NameServerClient> clusterConnectMap;

    private NameServerState state;

    private ClusterHost master;

    private ClusterHost serverHost;

    private long startTime;

    private NettyClientEventHandler eventHandler;

    public NameServerService(String clientId, String host, int port, String[] clusterHosts) {
        this.clientId = clientId;
        this.host = host;
        this.port = port;
        this.serverHost = new ClusterHost(clientId, host, port);

        //刚启动，master设为自己
        this.master = this.serverHost;

        this.clusterHosts = new ArrayList<>();
        for (String clusterHost : clusterHosts) {
            String[] split1 = clusterHost.split("@");
            String c = split1[0];

            String[] split = split1[1].split(":");
            String h = split[0];
            int p = Integer.parseInt(split[1]);

            this.clusterHosts.add(new ClusterHost(c, h, p));
        }

        this.clusterConnectMap = new ConcurrentHashMap<>();

        this.state = NameServerState.JUST_START;

        this.startTime = System.currentTimeMillis();

        init();
    }

    public void init() {
        eventHandler = new NettyClientEventHandler() {
            @Override
            public void activeHandler(ChannelHandlerContext ctx) {
            }

            @Override
            public void exceptionHandler(ChannelHandlerContext ctx, Throwable cause) {
                log.error("error", cause);
            }
        };

        NameServerProcessorAdaptor adaptor = new NameServerProcessorAdaptor(this, eventHandler);


        nameserverChannelFuture = new ServerBootstrap()
                .group(new NioEventLoopGroup(), new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, 65535)
                .childOption(ChannelOption.SO_RCVBUF, 65535)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new IdleStateHandler(NameServerConfig.SERVICE_OFF_LINE_TTL, 0, 0));

                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(NameServerConfig.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new LengthFieldPrepender(4));

                        ch.pipeline().addLast(new RemotingCommandDecoder());
                        ch.pipeline().addLast(new RemotingCommandEncoder());

                        ch.pipeline().addLast(adaptor);
                    }
                }).bind(this.host, this.port);
        connectNameServerCluster();
    }

    private void connectNameServerCluster() {
        //与其它的nameserver建立链接
        for (ClusterHost clusterHost : clusterHosts) {
            NameServerClient serverClient = new NameServerClient(this, clusterHost);
            clusterConnectMap.put(clusterHost, serverClient);
        }
    }


    public void publishNewMaster(VoteInfo voteInfo) {
        this.master = voteInfo.getVoteMaster();
        log.info("server [{}] vote [{}] be master", serverHost, voteInfo);
        clusterConnectMap.forEach((k,v)->{
            if(!k.equals(voteInfo.getVoteMaster())) {
                RemotingCommand command = MasterManagerSupport.voteRemoteMessageBuilder(startTime, serverHost);
                if(v.channel != null)
                    v.channel.writeAndFlush(command);
            }
        });
    }

    public void slaveOffLine(ClusterHost offLineHost) {
        if (offLineHost.equals(master)) {
            state = NameServerState.VOTE;
        }
    }
}
