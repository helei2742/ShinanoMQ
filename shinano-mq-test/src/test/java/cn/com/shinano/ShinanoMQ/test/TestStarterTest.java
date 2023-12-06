package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
import cn.hutool.json.JSONArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class TestStarterTest {

    private BufferedReader br;

    @BeforeEach
    void setUp() throws FileNotFoundException {

        String fileName = System.getProperty("user.dir") + File.separator + "needSend.txt";
        br = new BufferedReader(new FileReader(fileName));
    }

    @AfterEach
    void tearDown() {
    }



    @Test
    public void testQueryOffset() throws InterruptedException {
        ShinanoProducerClient client = new ShinanoProducerClient("localhost", 10022, "test-query-offset");
        client.run();
        System.out.println(queryOffset(client));
    }


    private long queryOffset(ShinanoProducerClient shinanoProducerClient) throws InterruptedException {
        RemotingCommand request = new RemotingCommand();
        request.setFlag(RemotingCommandFlagConstants.TOPIC_INFO_QUERY);

        request.addExtField(ExtFieldsConstants.TOPIC_QUERY_OPT_KEY, TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET);

        request.setTransactionId(UUID.randomUUID().toString());
        request.addExtField(ExtFieldsConstants.TOPIC_KEY, "test-create1");
        request.addExtField(ExtFieldsConstants.QUEUE_KEY, "queue1");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong res = new AtomicLong();

        shinanoProducerClient.sendMessage(request, remotingCommand->{
            System.out.println(remotingCommand);
            Long aLong = remotingCommand.getExtFieldsLong(TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET);
            System.out.println("--offset--" + aLong);
            latch.countDown();
        });

        latch.await();
        return res.get();
    }

    AtomicLong lastGetTime = new AtomicLong(0);

    @Test
    public void brokerTPTest() throws IOException, InterruptedException {
        int putThreadCount = 1;
        int threadPutMessageCount = 1000;

        Map<Integer, Integer> success = new HashMap<>();
        Map<Integer, Integer> fail = new HashMap<>();

        long start = System.currentTimeMillis();
        lastGetTime.set(start);
        sendMessage(
                putThreadCount,
                threadPutMessageCount,
                success,
                fail).await();

        long sendCost = System.currentTimeMillis() - start;

        TimeUnit.SECONDS.sleep(10);



        System.out.println(String.format("send msg over, total send [%d]",
                putThreadCount * threadPutMessageCount));

        System.out.println("success -- ");
        success.entrySet().forEach(e -> System.out.print(e + " "));
        System.out.println("-----------");

        System.out.println("fail -- ");
        fail.entrySet().forEach(e -> System.out.print(e + " "));
        System.out.println("-----------");

        System.out.println("send end cost " + sendCost + " total cost " + (lastGetTime.get() - start));

        TimeUnit.SECONDS.sleep(100);
    }


    private CountDownLatch sendMessage(int putThreadCount,
                                       int threadPutMessageCount,
                                       Map<Integer, Integer> successCounter,
                                       Map<Integer, Integer> failCounter) {

        CountDownLatch latch = new CountDownLatch(putThreadCount);

        AtomicInteger sendCount = new AtomicInteger(0);
        for (int i = 0; i < putThreadCount; i++) {
            int finalI = i;
            new Thread(() -> {
                ShinanoProducerClient client
                        = new ShinanoProducerClient("127.0.0.1", 10022, "client-" );

                client.run();

                for (int j = 0; j < threadPutMessageCount; j++) {

                    client.sendMessage(
                            "test-create1",
                            "queue1",
                            "thread-" + finalI + "-data-" + sendCount.incrementAndGet(),
                            remotingCommand -> {
                                lastGetTime.set(System.currentTimeMillis());
                                successCounter.compute(finalI, (k, v) -> {
                                    if (v == null) return 1;
                                    else return v + 1;
                                });
                            });
                }
                latch.countDown();
            }, "thread-" + finalI).start();
        }

        return latch;
    }
}
