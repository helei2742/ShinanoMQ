package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeMessage;
import cn.com.shinano.ShinanoMQ.consmer.dto.ConsumeResultState;
import cn.com.shinano.ShinanoMQ.consmer.listener.ConsumerOnMsgListener;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerTest {

    private static ShinanoConsumerClient consumerClient;


    static  {
        consumerClient = new ShinanoConsumerClient("127.0.0.1", 10022, "consumer-test-1");
        try {
            consumerClient.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getMessageTest() throws InterruptedException {

        TimeUnit.SECONDS.sleep(1);

        consumerClient.pullMessageAfterOffset("test-create1", "queue1", 0, 10);

        TimeUnit.SECONDS.sleep(2);
    }

    @Test
    public void testOnMessage() throws InterruptedException {
        final AtomicInteger total = new AtomicInteger(0);
        consumerClient.onMessage("test-create1", "queue1", new ConsumerOnMsgListener() {
            @Override
            public ConsumeResultState successHandler(ConsumeMessage message) {

                System.out.println(total.incrementAndGet() + "----" + message);
//                try {
//                    TimeUnit.SECONDS.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                return ConsumeResultState.FAIL;
            }

            @Override
            public void failHandler(Exception exception) {
                System.out.println(exception.getMessage());
            }
        });

        TimeUnit.SECONDS.sleep(4000);
    }

}
