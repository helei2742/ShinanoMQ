package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

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
        consumerClient.onMessage("test-create1", "queue1", saveMessage -> {
            System.out.println(saveMessage);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        TimeUnit.SECONDS.sleep(4000);
    }
}
