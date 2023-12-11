package cn.com.shinano.ShinanoMQ.base;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.constant.ClientStatus;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SendCommandFuture;
import cn.com.shinano.ShinanoMQ.base.idmaker.DistributeIdMaker;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.ClientInitMsgProcessor;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.NettyClientEventHandler;
import cn.com.shinano.ShinanoMQ.base.supporter.NettyChannelSendSupporter;
import cn.hutool.core.util.StrUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static cn.com.shinano.ShinanoMQ.base.constant.ClientStatus.*;


@Slf4j
public abstract class AbstractNettyClient {
    private String remoteHost;

    private int remotePort;

    private final String localHost;

    private final int localPort;

    private String clientId;

    protected ClientStatus status;

    private int idleTimeSeconds;

    private NettyClientEventHandler eventHandler;

    private ResultCallBackInvoker resultCallBackInvoker;

    private ClientInitMsgProcessor clientInitMsgProcessor;

    private AbstractNettyProcessorAdaptor nettyProcessorAdaptor;

    public Channel channel;

    private Bootstrap bootstrap;

    public AbstractNettyClient(String remoteHost, int remotePort) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.localHost = null;
        this.localPort = -1;
        this.status = CREATE_JUST;
    }

    public AbstractNettyClient(String remoteHost, int remotePort, String localHost, int localPort) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.localHost = localHost;
        this.localPort = localPort;

        this.status = CREATE_JUST;
    }

    public void init(String clientId,
                     Integer idleTimeSeconds,
                     ResultCallBackInvoker resultCallBackInvoker,
                     ClientInitMsgProcessor clientInitMsgProcessor,
                     AbstractNettyProcessorAdaptor nettyProcessorAdaptor,
                     NettyClientEventHandler eventHandler) {
        this.clientId = clientId;
        this.idleTimeSeconds = idleTimeSeconds;
        this.eventHandler = eventHandler;

        this.resultCallBackInvoker = resultCallBackInvoker;
        this.clientInitMsgProcessor = clientInitMsgProcessor;
        this.nettyProcessorAdaptor = nettyProcessorAdaptor;

        this.resultCallBackInvoker.init();
        this.nettyProcessorAdaptor.init(clientInitMsgProcessor, resultCallBackInvoker, eventHandler);
    }

    public void run() throws InterruptedException {
        NioEventLoopGroup group = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override//链接建立后被调用，进行初始化
                    protected void initChannel(NioSocketChannel ch) {
                        ch.pipeline().addLast(new IdleStateHandler(0, 0, idleTimeSeconds));

                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(ShinanoMQConstants.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new LengthFieldPrepender(4));

                        ch.pipeline().addLast(new RemotingCommandEncoder());
                        ch.pipeline().addLast(new RemotingCommandDecoder());

                        ch.pipeline().addLast(nettyProcessorAdaptor);
                    }
                });

        ChannelFuture channelFuture;
        if (StrUtil.isBlank(localHost)) {
            channelFuture = bootstrap.connect(new InetSocketAddress(remoteHost, remotePort));
        } else {
            channelFuture = bootstrap.connect(new InetSocketAddress(remoteHost, remotePort), new InetSocketAddress(localHost, localPort));
        }

        channelFuture.addListener(f -> {
            if (!f.isSuccess() && f.cause() instanceof ConnectException) {
                log.error("client init fail");
                status = START_FAILED;
            } else {
                log.info("client init success");
            }
        });

        this.channel = channelFuture.sync().channel();
        ChannelFuture closeFuture = channel.closeFuture();

        closeFuture.addListener((ChannelFutureListener) f -> {
            group.shutdownGracefully();
            closeFuture.channel().flush();
            eventHandler.closeHandler(f.channel());
        });
    }

    protected void reConnect(String remoteHost, int remotePort) throws InterruptedException {
        switch (status) {
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                status = CREATE_JUST;
                this.remoteHost = remoteHost;
                this.remotePort = remotePort;
                run();
                log.info("reconnect to [{}]-[{}]", remoteHost, remotePort);
                break;
            default:
                log.warn("client [{}] in [{}] state, can not recount [{}]:[{}]", clientId, status, remoteHost, remotePort);
        }
    }

    /**
     * 非阻塞发送消息
     *
     * @param request  request
     * @param success success callback
     * @param fail fail callback
     */
    public void sendMsg(RemotingCommand request, Consumer<RemotingCommand> success, Consumer<RemotingCommand> fail) {
        if (request.getTransactionId() == null) {
            request.setTransactionId(DistributeIdMaker.DEFAULT.nextId(clientId));
        }
        request.setClientId(this.clientId);
        resultCallBackInvoker.addAckListener(request.getTransactionId(), success, fail);
        log.debug("send remotingCommand [{}]", request);
        NettyChannelSendSupporter.sendMessage(request, channel);
    }

    /**
     * 阻塞发送消息
     *
     * @param request request
     * @return boolean
     * @throws InterruptedException 阻塞过程中可能出现打断异常
     */
    public boolean sendMsg(RemotingCommand request) throws InterruptedException {
        SendCommandFuture result = new SendCommandFuture();

        sendMsg(request, result::setResult, result::setResult);
        Object o = result.getResult();
        return o != null && ((RemotingCommand) o).getCode() != RemotingCommandCodeConstants.FAIL;
    }

    /**
     * 发送消息，返回Future
     * @param request request
     * @return SendCommandFuture
     */
    public SendCommandFuture sendMsgFuture(RemotingCommand request) {
        SendCommandFuture result = new SendCommandFuture();

        sendMsg(request, result::setResult, result::setResult);
        return result;
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
                    log.warn("client [{}] in [{}] state, remote[{}]:[{}], can not turn to active state", clientId, status, remoteHost, remotePort);
            }
        }

        protected void sendInitMessage(ChannelHandlerContext ctx) {
        }

        @Override
        public void closeHandler(Channel channel) {
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
