package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.TopicQueryOPT;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
import cn.hutool.core.util.RandomUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
            for (int i = 0; i < 5; i++) {
                String finalLine = line;
                int finalI = i;
                new Thread(()->{
                    for (int j = 0; j < 10; j++) {
                        Message message = new Message();
                        message.setFlag(MessageOPT.PRODUCER_MESSAGE);
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
        message.setFlag(MessageOPT.TOPIC_INFO_QUERY);
        Map<String, String> prop = new HashMap<>();
        prop.put(TopicQueryOPT.TOPIC_QUERY_OPT_KEY, TopicQueryOPT.QUERY_TOPIC_QUEUE_OFFSET);
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
        message.setFlag(MessageOPT.TOPIC_INFO_QUERY);
        Map<String, String> prop = new HashMap<>();
        prop.put(TopicQueryOPT.TOPIC_QUERY_OPT_KEY, TopicQueryOPT.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE);
        message.setProperties(prop);
        message.setTopic("test-create1");
        message.setQueue("queue1");
        message.setBody("3630".getBytes(StandardCharsets.UTF_8));
        shinanoProducerClient.sendMsg(message);
        TimeUnit.SECONDS.sleep(500);
    }
}
