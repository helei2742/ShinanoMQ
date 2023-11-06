package cn.com.shinano.ShinanoMQ.producer.executor;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Deprecated
public class SendExecutor {
    private static final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static final LinkedBlockingQueue<Object> msgQueue = new LinkedBlockingQueue<>();
    private Channel channel;

    public SendExecutor(Channel channel) {
        this.channel = channel;

        executorService.execute(()->{
            while(true) {
                Object poll = null;
                try {
                    poll = msgQueue.take();
                } catch (InterruptedException e) {
                    log.error("error when add message into queue", e);
                }
                channel.writeAndFlush(poll);
                log.debug("send message {}", poll);
            }
        });
    }

    public void sendMessage(Object msg) {
        msgQueue.add(msg);
    }
}
