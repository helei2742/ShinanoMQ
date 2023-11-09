package cn.com.shinano.ShinanoMQ.core;

import cn.com.shinano.ShinanoMQ.base.MessageDecoder;
import cn.com.shinano.ShinanoMQ.base.MessageEncoder;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.BootstrapHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class ShinanoMQBroker implements ApplicationRunner {

    private EventLoopGroup resolveMessageGroup;

    private ChannelFuture channelFuture;

    @Value("${shinano.mq.broker.port}")
    private Integer port;

    @Autowired
    private BootstrapHandler bootstrapHandler;

    public void init() {
        resolveMessageGroup = new DefaultEventLoopGroup(BrokerConfig.BOOTSTRAP_HANDLER_THREAD);

        channelFuture = new ServerBootstrap()
                .group(new NioEventLoopGroup(1), new NioEventLoopGroup())
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
                        ch.pipeline().addLast(new IdleStateHandler(BrokerConfig.CLIENT_OFF_LINE_INTERVAL,0, 0));

                        ch.pipeline().addLast(new MessageDecoder());
                        ch.pipeline().addLast(new MessageEncoder());

                        ch.pipeline().addLast(resolveMessageGroup, "bootstrapHandler", bootstrapHandler);
                    }
                }).bind(this.port);

        log.info("shinano mq broker start port {}", port);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.init();
    }
}
