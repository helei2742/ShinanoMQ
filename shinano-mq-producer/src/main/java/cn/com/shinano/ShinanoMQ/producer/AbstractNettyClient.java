package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.MessageDecoder;
import cn.com.shinano.ShinanoMQ.base.MessageEncoder;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.producer.config.ProducerConfig;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.producer.processor.ProducerBootstrapProcessor;
import cn.com.shinano.ShinanoMQ.producer.processor.msgprocessor.ProducerClientInitProcessor;
import cn.com.shinano.ShinanoMQ.producer.processor.msgprocessor.ReceiveMessageProcessor;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
public abstract class AbstractNettyClient {
    private final String host;
    private final int port;
    private String clientId;

    private Channel channel;

    private NettyClientEventHandler eventHandler;

    private ProducerBootstrapProcessor producerBootstrapProcessor;

    private final ReceiveMessageProcessor receiveMessageProcessor;

    private final ClientInitMsgProcessor clientInitMsgProcessor;

    public AbstractNettyClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.receiveMessageProcessor = new ReceiveMessageProcessor();
        this.clientInitMsgProcessor = new ProducerClientInitProcessor();
    }

    public void init(String clientId, NettyClientEventHandler eventHandler) {
        this.clientId = clientId;
        this.eventHandler = eventHandler;
    }

    public void run() throws InterruptedException {
        NioEventLoopGroup group = new NioEventLoopGroup();

        this.producerBootstrapProcessor = new ProducerBootstrapProcessor(clientId);
        ChannelFuture channelFuture = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override//链接建立后被调用，进行初始化
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new IdleStateHandler(0, 0, ProducerConfig.IDLE_TIME_SECONDS));

                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(ShinanoMQConstants.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new LengthFieldPrepender(4));

                        ch.pipeline().addLast(new MessageEncoder());
                        ch.pipeline().addLast(new MessageDecoder());

                        ch.pipeline().addLast(producerBootstrapProcessor);
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

                eventHandler.closeHandler();

                log.info("connect to broker off");
            }
        });

        this.receiveMessageProcessor.init();

        this.producerBootstrapProcessor.init(
                channel,
                this.clientInitMsgProcessor,
                this.receiveMessageProcessor,
                this.eventHandler
        );

        log.info("producer channel init success");
    }

    public void sendMsg(Message msg) {
        msg.setTransactionId(UUID.randomUUID().toString());

        producerBootstrapProcessor.sendMsg(msg);
    }

    public void sendMsg(Message msg, Consumer<Message> success) {
        msg.setTransactionId(UUID.randomUUID().toString());

        receiveMessageProcessor.addAckListener(msg.getTransactionId(), success);
        producerBootstrapProcessor.sendMsg(msg);
    }

    public void sendMsg(Message msg, Consumer<Message> success, Consumer<Message> fail) {
        msg.setTransactionId(UUID.randomUUID().toString());

        receiveMessageProcessor.addAckListener(msg.getTransactionId(), success, fail);
        producerBootstrapProcessor.sendMsg(msg);
    }
}
