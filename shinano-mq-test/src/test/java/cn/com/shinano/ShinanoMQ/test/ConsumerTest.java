package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class ConsumerTest {

    @Test
    public void getMessageTest() throws InterruptedException {
        ShinanoConsumerClient consumerClient = new ShinanoConsumerClient("127.0.0.1", 10022, "consumer-1");
        consumerClient.run();

        TimeUnit.SECONDS.sleep(1);

        consumerClient.queryMessageAfterOffset("test-create1", "queue1", 0, 10);


        TimeUnit.SECONDS.sleep(2);
        consumerClient.show();
    }
}
