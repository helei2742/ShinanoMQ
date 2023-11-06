package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageDecoder;
import cn.com.shinano.ShinanoMQ.base.MessageEncoder;
import cn.com.shinano.ShinanoMQ.base.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.producer.executor.SendExecutor;
import cn.com.shinano.ShinanoMQ.producer.nettyhandler.ProducerBootstrapHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

@Slf4j
public class ShinanoProducerClient {
    private final String host;
    private final int port;
    private Channel channel;

    private ProducerBootstrapHandler producerBootstrapHandler;

    private SendExecutor sendExecutor;

    public ShinanoProducerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void run() throws InterruptedException {
        NioEventLoopGroup group = new NioEventLoopGroup();


        this.producerBootstrapHandler = new ProducerBootstrapHandler();
        ChannelFuture channelFuture = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override//链接建立后被调用，进行初始化
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new IdleStateHandler(0, 0, ProducerConfig.IDLE_TIME_SECONDS));

                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(ShinanoMQConstants.MAX_FRAME_LENGTH,0,2,0,2));
                        ch.pipeline().addLast(new LengthFieldPrepender(2));

                        ch.pipeline().addLast(new MessageEncoder());
                        ch.pipeline().addLast(new MessageDecoder());


                        ch.pipeline().addLast(producerBootstrapHandler);
                    }
                })
                //1、链接到服务器，
                //异步非阻塞，main发起调用，真正执行connect是nio线程
                .connect(new InetSocketAddress(host, port));

        channel = channelFuture.sync().channel();

        ChannelFuture closeFuture = channel.closeFuture();

        closeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                group.shutdownGracefully();
                closeFuture.channel().flush();
                log.debug("connect to broker off");
            }
        });

        producerBootstrapHandler.init(channel);

        log.debug("channel init success");
//        sendExecutor = new SendExecutor(channel);
    }

    public void sendMsg(Message msg) {
//        sendExecutor.sendMessage(msg);
        producerBootstrapHandler.sendMsg(msg);
    }
}
