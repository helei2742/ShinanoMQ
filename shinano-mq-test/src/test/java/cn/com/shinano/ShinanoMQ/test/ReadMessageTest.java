package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.dto.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
public class ReadMessageTest {


    @Test
    public void getMessageTest() throws InterruptedException {
        ShinanoProducerClient client
                = new ShinanoProducerClient("127.0.0.1", 10022);

        client.run();

//        long l = queryOffset(client);
//        System.out.println(l);

        System.out.println(queryMessage(client, 0, 12));
    }

    private MessageListVO queryMessage(ShinanoProducerClient shinanoProducerClient, long offset, int count) throws InterruptedException {
        Message message = new Message();
        message.setFlag(MessageOPT.TOPIC_INFO_QUERY);

        Map<String, String> prop = new HashMap<>();
        prop.put(TopicQueryConstants.TOPIC_QUERY_OPT_KEY, TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE);
        prop.put(TopicQueryConstants.QUERY_TOPIC_MESSAGE_COUNT_KEY, String.valueOf(count));
        message.setProperties(prop);

        message.setTopic("test-create1");
        message.setQueue("queue1");
        message.setBody(String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
        message.setTransactionId(UUID.randomUUID().toString());

        CountDownLatch latch = new CountDownLatch(1);
        final MessageListVO[] res = {null};

        shinanoProducerClient.sendMsg(message, msg->{
            byte[] array = ByteBuffer.wrap(msg.getBody()).array();
            res[0] = ProtostuffUtils.deserialize(array, MessageListVO.class);
            latch.countDown();
        });

        latch.await();
        return res[0];
    }


    private long queryOffset(ShinanoProducerClient shinanoProducerClient) throws InterruptedException {
        Message message = new Message();
        message.setFlag(MessageOPT.TOPIC_INFO_QUERY);
        Map<String, String> prop = new HashMap<>();
        prop.put(TopicQueryConstants.TOPIC_QUERY_OPT_KEY, TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET);
        message.setProperties(prop);
        message.setTopic("test-create1");
        message.setQueue("queue1");
        message.setBody("123-client".getBytes(StandardCharsets.UTF_8));
        message.setTransactionId(UUID.randomUUID().toString());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong res = new AtomicLong();

        shinanoProducerClient.sendMsg(message, msg->{
            res.set(ByteBuffer.wrap(msg.getBody()).getLong());
            latch.countDown();
        });

        latch.await();
        return res.get();
    }
}
