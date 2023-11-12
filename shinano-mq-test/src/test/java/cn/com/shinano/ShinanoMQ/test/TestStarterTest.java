package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.dto.*;
import cn.com.shinano.ShinanoMQ.base.dto.MsgFlagConstants;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
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
    public void test() throws InterruptedException, IOException {
//        ShinanoMQServer shinanoMQServer = new ShinanoMQServer(10011);
//        shinanoMQServer.run();

        ShinanoProducerClient shinanoProducerClient
                = new ShinanoProducerClient("127.0.0.1", 10022);
        shinanoProducerClient.run();

        String line = null;
        AtomicInteger atomicInteger = new AtomicInteger(0);
        while ((line = br.readLine()) != null) {
            for (int i = 0; i < 1; i++) {
                String finalLine = line;
                int finalI = i;
                new Thread(()->{
                    for (int j = 0; j < 1; j++) {
                        Message message = new Message();
                        message.setFlag(MsgFlagConstants.PRODUCER_MESSAGE);
                        message.setTopic("test-create1");
                        message.setQueue("queue1");
//                        message.setValue("test-line-" + finalLine + "-" + atomicInteger.incrementAndGet());
                        message.setBody(("test-line-" + atomicInteger.incrementAndGet()).getBytes(StandardCharsets.UTF_8));
                        message.setTransactionId(UUID.randomUUID().toString());
                        shinanoProducerClient.sendMsg(message);
                        try {
                            TimeUnit.MILLISECONDS.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
                break;
            }
        }

        TimeUnit.SECONDS.sleep(500);
    }

    @Test
    public void testQueryOffset() throws InterruptedException {
        ShinanoProducerClient shinanoProducerClient
                = new ShinanoProducerClient("localhost", 10022);
        shinanoProducerClient.run();
        Message message = new Message();
        message.setFlag(MsgFlagConstants.TOPIC_INFO_QUERY);
        Map<String, String> prop = new HashMap<>();
        prop.put(MsgPropertiesConstants.TOPIC_QUERY_OPT_KEY, TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET);
        message.setProperties(prop);
        message.setTopic("test-create1");
        message.setQueue("queue1");
        message.setBody("123-client".getBytes(StandardCharsets.UTF_8));
        shinanoProducerClient.sendMsg(message);
        TimeUnit.SECONDS.sleep(100);
    }

    @Test
    public void testQueryTopicQueueMsg() throws InterruptedException {
        ShinanoProducerClient shinanoProducerClient
                = new ShinanoProducerClient("localhost", 10022);
        shinanoProducerClient.run();
        Message message = new Message();
        message.setFlag(MsgFlagConstants.TOPIC_INFO_QUERY);
        Map<String, String> prop = new HashMap<>();
        prop.put(MsgPropertiesConstants.TOPIC_QUERY_OPT_KEY, TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE);
        message.setProperties(prop);
        message.setTopic("test-create1");
        message.setQueue("queue1");
        message.setBody("3630".getBytes(StandardCharsets.UTF_8));
        shinanoProducerClient.sendMsg(message);
        TimeUnit.SECONDS.sleep(500);
    }

    AtomicLong lastGetTime = new AtomicLong(0);

    @Test
    public void brokerTPTest() throws IOException, InterruptedException {
        int putThreadCount = 10;
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

        TimeUnit.SECONDS.sleep(5);

        System.out.println(String.format("send msg over, total send [%d]",
                putThreadCount*threadPutMessageCount));

        System.out.println("success -- ");
        success.entrySet().forEach(e-> System.out.print(e+" "));
        System.out.println("-----------");

        System.out.println("fail -- ");
        fail.entrySet().forEach(e-> System.out.print(e+" "));
        System.out.println("-----------");

        System.out.println("send end cost " + sendCost + " total cost " + (lastGetTime.get()-start));
    }


    private CountDownLatch sendMessage(int putThreadCount,
                                       int threadPutMessageCount,
                                       Map<Integer, Integer> successCounter,
                                       Map<Integer, Integer>  failCounter){

        CountDownLatch latch = new CountDownLatch(putThreadCount);

        AtomicInteger sendCount = new AtomicInteger(0);
        for (int i = 0; i < putThreadCount; i++) {
            int finalI = i;
            new Thread(()->{
                ShinanoProducerClient client
                        = new ShinanoProducerClient("127.0.0.1", 10022);
                try {
                    client.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (int j = 0; j < threadPutMessageCount; j++) {
                    Message message = new Message();
                    message.setFlag(MsgFlagConstants.PRODUCER_MESSAGE);
                    message.setTopic("test-create1");
                    message.setQueue("queue1");
//                        message.setValue("test-line-" + finalLine + "-" + atomicInteger.incrementAndGet());
                    message.setBody(("thread-"+finalI+"-data-" + sendCount.incrementAndGet()).getBytes(StandardCharsets.UTF_8));
                    message.setTransactionId(UUID.randomUUID().toString());

                    client.sendMsg(message, msg->{
                        lastGetTime.set(System.currentTimeMillis());
                        successCounter.compute(finalI, (k,v)->{
                            if(v == null) return 1;
                            else return v+1;
                        });
                    }, msg-> {
                        lastGetTime.set(System.currentTimeMillis());
                        failCounter.compute(finalI, (k,v)->{
                            if(v == null) return 1;
                            else return v+1;
                        });
                    });
//                    try {
//                        TimeUnit.MILLISECONDS.sleep(10);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }
                latch.countDown();
            }, "thread-" + finalI).start();
        }

        return latch;
    }
}
