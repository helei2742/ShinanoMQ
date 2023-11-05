package cn.com.shinano.ShinanoMQ.core;

import cn.com.shinano.ShinanoMQ.base.MessageDecoder;
import cn.com.shinano.ShinanoMQ.base.MessageEncoder;
import cn.com.shinano.ShinanoMQ.core.nettyhandler.BootstrapHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
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
        resolveMessageGroup = new DefaultEventLoopGroup();

        channelFuture = new ServerBootstrap()
                .group(new NioEventLoopGroup(), new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024, 0, 2, 0, 2));
                        ch.pipeline().addLast(new LengthFieldPrepender(2));

                        ch.pipeline().addLast(new MessageDecoder());
                        ch.pipeline().addLast(new MessageEncoder());

                        ch.pipeline().addLast(resolveMessageGroup,
                                "bootstrapHandler", bootstrapHandler);
                    }
                }).bind(this.port);

        log.info("shinano mq broker start port {}", port);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.init();
    }

}
