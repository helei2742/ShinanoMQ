package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.sql.Time;
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
                    for (int j = 0; j < 20; j++) {
                        Message message = new Message();
                        message.setOpt(MessageOPT.PRODUCER_MESSAGE);
                        message.setTopic("test-topic");
                        message.setQueue("test-queue");
//                        message.setValue("test-line-" + finalLine + "-" + atomicInteger.incrementAndGet());
                        message.setValue("test-line-" + atomicInteger.incrementAndGet());
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
                = new ShinanoProducerClient("localhost", 10021);
        shinanoProducerClient.run();
        Message message = new Message();
        message.setOpt(MessageOPT.BROKER_INFO_QUERY);
        message.setTopic("test-topic");
        message.setQueue("test-queue");
        message.setValue("123-client");
        shinanoProducerClient.sendMsg(message);
        TimeUnit.SECONDS.sleep(100);
    }

    @Test
    public void testQueryTopicQueueMsg() throws InterruptedException {
        ShinanoProducerClient shinanoProducerClient
                = new ShinanoProducerClient("localhost", 10022);
        shinanoProducerClient.run();
        Message message = new Message();
        message.setOpt(MessageOPT.TOPIC_QUEUE_OFFSET_MESSAGE_QUERY);
        message.setTopic("test-topic");
        message.setQueue("test-queue");
        message.setValue("83");
        shinanoProducerClient.sendMsg(message);
        TimeUnit.SECONDS.sleep(500);
    }
}
