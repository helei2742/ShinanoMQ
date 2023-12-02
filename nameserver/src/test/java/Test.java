import cn.com.shinano.ShinanoMQ.base.RemotingCommandDecoder;
import cn.com.shinano.ShinanoMQ.base.RemotingCommandEncoder;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.nettyhandler.AbstractNettyProcessorAdaptor;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.nameserver.NameServerService;
import cn.com.shinano.ShinanoMQ.base.dto.ServiceRegistryDTO;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
public class Test {

    @org.junit.Test
    public void testStart1() throws InterruptedException {
//        NameServerService nameServerService1 = new NameServerService("server1", "localhost",
//                10001, new String[]{"server2@localhost:10002", "server3@localhost:10003"});
        NameServerService nameServerService1 = new NameServerService("server1", "localhost",
                10001, new String[]{});

        while (true) {
//            System.out.println(nameServerService1.getMaster());
//            System.out.println(nameServerService1.getClusterConnectMap());
            TimeUnit.SECONDS.sleep(10);
        }

    }
    @org.junit.Test
    public void testStart2() throws InterruptedException {
        NameServerService nameServerService2 = new NameServerService("server2", "localhost",
                10002, new String[]{"server1@localhost:10001", "server3@localhost:10003"});
        while (true) {
//            System.out.println(nameServerService2.getMaster());
//            System.out.println(nameServerService2.getClusterConnectMap());
            TimeUnit.SECONDS.sleep(10);
        }
    }
    @org.junit.Test
    public void testStart3() throws InterruptedException {
        NameServerService nameServerService3 = new NameServerService("server3", "localhost",
                10003, new String[]{"server2@localhost:10002", "server1@localhost:10001"});
        while (true) {
//            System.out.println(nameServerService3.getMaster());
//            System.out.println(nameServerService3.getClusterConnectMap());
            TimeUnit.SECONDS.sleep(10);
        }
    }

    @org.junit.Test
    public void testRegistry() throws InterruptedException {
        ChannelFuture channelFuture = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override//链接建立后被调用，进行初始化
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new IdleStateHandler(0, 0, 30));

                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(ShinanoMQConstants.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new LengthFieldPrepender(4));

                        ch.pipeline().addLast(new RemotingCommandEncoder());
                        ch.pipeline().addLast(new RemotingCommandDecoder());

                        ch.pipeline().addLast(new AbstractNettyProcessorAdaptor() {
                            @Override
                            protected void handlerMessage(ChannelHandlerContext context, RemotingCommand remotingCommand) {
                                System.out.println(remotingCommand);
                            }

                            @Override
                            public void printLog(String logStr) {

                            }
                        });
                    }
                })
                .connect(new InetSocketAddress("127.0.0.1", 10001));


        Channel channel = channelFuture.sync().channel();
        ChannelFuture closeFuture =  channel.closeFuture();


        RemotingCommand remotingCommand = new RemotingCommand();

        ServiceRegistryDTO serviceRegistryDTO = new ServiceRegistryDTO("test-mq", null,null);
        serviceRegistryDTO.setClientId("broker-1");
        serviceRegistryDTO.setAddress("localhost");
        serviceRegistryDTO.setPort(8888);
        ;
        remotingCommand.setFlag(RemotingCommandFlagConstants.CLIENT_REGISTRY_SERVICE);
        remotingCommand.setTransactionId(UUID.randomUUID().toString());
        remotingCommand.setBody(ProtostuffUtils.serialize(serviceRegistryDTO));

        channel.writeAndFlush(remotingCommand);


        TimeUnit.SECONDS.sleep(1000);
    }
}
