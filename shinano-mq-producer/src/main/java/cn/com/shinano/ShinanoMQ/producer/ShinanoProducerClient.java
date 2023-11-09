package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.MessageDecoder;
import cn.com.shinano.ShinanoMQ.base.MessageEncoder;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.producer.nettyhandler.ProducerBootstrapHandler;
import cn.com.shinano.ShinanoMQ.producer.service.ResultCallBackInvoker;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
public class ShinanoProducerClient {
    private final String host;
    private final int port;
    private Channel channel;

    private ProducerBootstrapHandler producerBootstrapHandler;


    private ResultCallBackInvoker resultCallBackInvoker;

    public ShinanoProducerClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.resultCallBackInvoker = new ResultCallBackInvoker();

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

                        ch.pipeline().addLast(new MessageEncoder());
                        ch.pipeline().addLast(new MessageDecoder());

                        ch.pipeline().addLast(producerBootstrapHandler);
                    }
                })
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

        this.resultCallBackInvoker.init();
        this.producerBootstrapHandler.init(channel, this.resultCallBackInvoker);

        log.debug("channel init success");
    }

    public void sendMsg(Message msg) {
        producerBootstrapHandler.sendMsg(msg);
    }

    public void sendMsg(Message msg, Consumer<Message> success) {
        msg.setTransactionId(UUID.randomUUID().toString());
        resultCallBackInvoker.addAckListener(msg.getTransactionId(), success);
        producerBootstrapHandler.sendMsg(msg);
    }

    public void sendMsg(Message msg, Consumer<Message> success, Consumer<Message> fail) {
        msg.setTransactionId(UUID.randomUUID().toString());
        resultCallBackInvoker.addAckListener(msg.getTransactionId(), success, fail);
        producerBootstrapHandler.sendMsg(msg);
    }
}
