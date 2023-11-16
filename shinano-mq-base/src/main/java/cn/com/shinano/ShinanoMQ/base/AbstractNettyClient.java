package cn.com.shinano.ShinanoMQ.base;

import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
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

    private int idleTimeSeconds;

    private NettyClientEventHandler eventHandler;

    private ReceiveMessageProcessor receiveMessageProcessor;

    private ClientInitMsgProcessor clientInitMsgProcessor;

    private AbstractNettyProcessorAdaptor nettyProcessorAdaptor;

    protected Channel channel;


    public AbstractNettyClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void init(String clientId,
                     Integer idleTimeSeconds,
                     ReceiveMessageProcessor receiveMessageProcessor,
                     ClientInitMsgProcessor clientInitMsgProcessor,
                     AbstractNettyProcessorAdaptor nettyProcessorAdaptor,
                     NettyClientEventHandler eventHandler) {
        this.clientId = clientId;
        this.idleTimeSeconds = idleTimeSeconds;
        this.eventHandler = eventHandler;
        this.receiveMessageProcessor = receiveMessageProcessor;
        this.clientInitMsgProcessor = clientInitMsgProcessor;
        this.nettyProcessorAdaptor = nettyProcessorAdaptor;
    }

    public void run() throws InterruptedException {
        NioEventLoopGroup group = new NioEventLoopGroup();

        ChannelFuture channelFuture = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override//链接建立后被调用，进行初始化
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new IdleStateHandler(0, 0, idleTimeSeconds));

                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(ShinanoMQConstants.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new LengthFieldPrepender(4));

                        ch.pipeline().addLast(new RemotingCommandEncoder());
                        ch.pipeline().addLast(new RemotingCommandDecoder());

                        ch.pipeline().addLast(nettyProcessorAdaptor);
                    }
                })
                .connect(new InetSocketAddress(host, port));


        this.channel = channelFuture.sync().channel();
        ChannelFuture closeFuture =  channel.closeFuture();

        closeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                group.shutdownGracefully();
                closeFuture.channel().flush();

                eventHandler.closeHandler();
            }
        });
    }


    protected void sendMsg(RemotingCommand remotingCommand, Consumer<RemotingCommand> success, Consumer<RemotingCommand> fail) {
        remotingCommand.setTransactionId(UUID.randomUUID().toString());

        receiveMessageProcessor.addAckListener(remotingCommand.getTransactionId(), success, fail);


    }
}
