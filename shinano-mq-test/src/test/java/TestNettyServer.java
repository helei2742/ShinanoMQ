
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lhe.shinano
 * @date 2023/12/5
 */
@Slf4j
public class TestNettyServer {

    @Test
    public void server() throws InterruptedException {
        ChannelFuture future = new ServerBootstrap()
                .group(new NioEventLoopGroup(), new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new StringDecoder());
                        ch.pipeline().addLast(new StringEncoder());

                        ch.pipeline().addLast(new SimpleChannelInboundHandler<String>() {

                            @Override
                            protected void channelRead0(ChannelHandlerContext channelHandlerContext, String str) throws Exception {
                                TimeUnit.SECONDS.sleep(2);
                                log.info("service got message [{}]", str);
                                channelHandlerContext.channel().writeAndFlush("service send message");
                            }
                        });
                    }
                }).bind(10111);

        TimeUnit.SECONDS.sleep(1000);
    }

    public static void main(String[] args) {
        Bootstrap handler = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override//链接建立后被调用，进行初始化
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new StringDecoder());
                        ch.pipeline().addLast(new StringEncoder());
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<String>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext channelHandlerContext, String str) throws Exception {

                                log.info("client get message [{}]", str);
                            }
                        });
                    }
                });

        AtomicReference<ChannelFuture> future = new AtomicReference<>(handler.connect(new InetSocketAddress("localhost", 10111)));

        future.get().addListener(f->{
            if (!f.isSuccess() && f.cause() instanceof ConnectException) {
               log.error("client init fail");
               f.cause().printStackTrace();
            } else {
               log.info("client init success");
            }
        });

        Scanner scanner = new Scanner(System.in);
        String line = null;

        while (!"exit".equals(line)) {
            line = scanner.nextLine();
            future.get().channel().writeAndFlush(line).addListener(f->{
                if (f.isSuccess()) {
                    log.info("client send success");
                    return;
                }
                log.warn("client send a request command to channel failed.");

                future.set(handler.connect(new InetSocketAddress("localhost", 10111)));
            });
        }
    }
}
