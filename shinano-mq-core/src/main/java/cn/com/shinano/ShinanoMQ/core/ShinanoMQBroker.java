package cn.com.shinano.ShinanoMQ.core;

import cn.com.shinano.ShinanoMQ.base.RemotingCommandDecoder;
import cn.com.shinano.ShinanoMQ.base.RemotingCommandEncoder;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.manager.NameServerManager;
import cn.com.shinano.ShinanoMQ.core.manager.cluster.MessageInstanceSyncSupport;
import cn.com.shinano.ShinanoMQ.core.processor.BrokerMessageProcessorAdaptor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Slf4j
@Component
public class ShinanoMQBroker implements ApplicationRunner {

    private EventLoopGroup resolveMessageGroup;

    private ChannelFuture channelFuture;

    private HashedWheelTimer wheelTimer =
            new HashedWheelTimer(Executors.defaultThreadFactory(), 1000L, TimeUnit.MILLISECONDS, 600, true,-1L,Executors.newFixedThreadPool(1));

    @Autowired
    private BrokerSpringConfig brokerSpringConfig;

    @Autowired
    private BrokerMessageProcessorAdaptor brokerMessageProcessorAdaptor;

    @Autowired
    private NameServerManager nameServerManager;

    @Autowired
    private MessageInstanceSyncSupport instanceSyncSupport;

    public void init() {
        nameServerManager.init(nameServerClient -> {
            nameServerManager.startServiceDiscover(brokerSpringConfig.getServiceId());

            if (nameServerManager.SLAVE_KEY.equals(brokerSpringConfig.getType())) {
                wheelTimer.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        instanceSyncSupport.slaveSyncTopicInfoToMasterStart();
                    }
                }, BrokerConfig.SLAVE_BROKER_SYNC_TOPIC_INFO_TO_MASTER_INTERVAL, TimeUnit.MILLISECONDS);
            }
        });

        resolveMessageGroup = new DefaultEventLoopGroup(BrokerConfig.BOOTSTRAP_HANDLER_THREAD);

        channelFuture = new ServerBootstrap()
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
                        ch.pipeline().addLast(new IdleStateHandler(BrokerConfig.CLIENT_OFF_LINE_INTERVAL, 0, 0));

                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(ShinanoMQConstants.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new LengthFieldPrepender(4));

                        ch.pipeline().addLast(new RemotingCommandDecoder());
                        ch.pipeline().addLast(new RemotingCommandEncoder());

                        ch.pipeline().addLast(resolveMessageGroup, "bootstrapHandler", brokerMessageProcessorAdaptor);
                    }
                }).bind(brokerSpringConfig.getPort());

        log.info("shinano mq broker start port {}", brokerSpringConfig.getPort());
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.init();
    }
}
