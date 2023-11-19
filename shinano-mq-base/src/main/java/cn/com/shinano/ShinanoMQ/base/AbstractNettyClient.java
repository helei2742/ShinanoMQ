package cn.com.shinano.ShinanoMQ.base;

import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.constant.ClientStatus;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
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

import static cn.com.shinano.ShinanoMQ.base.constant.ClientStatus.CREATE_JUST;
import static cn.com.shinano.ShinanoMQ.base.constant.ClientStatus.RUNNING;


@Slf4j
public abstract class AbstractNettyClient {
    private final String host;

    private final int port;

    private String clientId;

    protected ClientStatus status;

    private int idleTimeSeconds;

    private NettyClientEventHandler eventHandler;

    private ReceiveMessageProcessor receiveMessageProcessor;

    private ClientInitMsgProcessor clientInitMsgProcessor;

    private AbstractNettyProcessorAdaptor nettyProcessorAdaptor;

    private Channel channel;


    public AbstractNettyClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.status = CREATE_JUST;
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

        this.receiveMessageProcessor.init();
        this.nettyProcessorAdaptor.init(clientInitMsgProcessor, receiveMessageProcessor, eventHandler);
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


    public void sendMsg(RemotingCommand remotingCommand, Consumer<RemotingCommand> success, Consumer<RemotingCommand> fail) {
        remotingCommand.setTransactionId(UUID.randomUUID().toString());
        receiveMessageProcessor.addAckListener(remotingCommand.getTransactionId(), success, fail);
        NettyChannelSendSupporter.sendMessage(remotingCommand, channel);
        log.debug("send remotingCommand [{}]", remotingCommand);
    }


    public class DefaultNettyEventClientHandler implements NettyClientEventHandler {
        @Override
        public void activeHandler(ChannelHandlerContext ctx) {
            switch (status) {
                case CREATE_JUST:
                case START_FAILED:
                    sendInitMessage(ctx);
                    break;
                default:
                    log.warn("client [{}] in [{}] state, can not turn to active state", clientId, status);
            }
        }

        protected void sendInitMessage(ChannelHandlerContext ctx) {
        }

        @Override
        public void closeHandler() {
            switch (status) {
                case CREATE_JUST:
                case RUNNING:
                    status = ClientStatus.SHUTDOWN_ALREADY;
                    break;
                default:
                    log.warn("client [{}] in [{}] state, can not turn to close state", clientId, status);
            }
        }

        @Override
        public void initSuccessHandler(RemotingCommand remotingCommand) {
            if (status == CREATE_JUST) {
                clientInit(remotingCommand);
                status = RUNNING;
            } else {
                log.warn("client [{}] in [{}] state, can not turn to running state", clientId, status);
            }
        }

        protected void clientInit(RemotingCommand remotingCommand) {
        }

        @Override
        public void initFailHandler() {

            if (status == CREATE_JUST) {
                status = ClientStatus.START_FAILED;
            } else {
                log.warn("client [{}] in [{}] state, can not turn to start failed state", clientId, status);
            }
        }

        @Override
        public void exceptionHandler(ChannelHandlerContext ctx, Throwable cause) {
            log.error("producer got an error", cause);
            status = ClientStatus.SHUTDOWN_ALREADY;
        }
    }

}
