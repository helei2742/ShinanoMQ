package cn.com.shinano.ShinanoMQ.test;

import cn.com.shinano.ShinanoMQ.base.constans.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.dto.*;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.producer.ShinanoProducerClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
public class ReadMessageTest {


    @Test
    public void getMessageTest() throws InterruptedException {
        ShinanoProducerClient client
                = new ShinanoProducerClient("127.0.0.1", 10023, "client-query" );

        client.run();

//        long p = 916685;
//        long p = 333680;
//        long p = 1032500;
        long p = 0;
        int total = 0;
        long start = System.currentTimeMillis();
        int sleep = 0;
        Set<String> set = new HashSet<>();
        while (true) {
            MessageListVO x = queryMessage(client, p, 200);
            List<SaveMessage> messages = x.getMessages();
            if(messages == null || messages.size() == 0) break;
            p = x.getNextOffset();
            total += messages.size();
            sleep++;

            for (SaveMessage message : messages) {
//                System.out.println(ProtostuffUtils.deserialize(message.getBody(), RetryMessage.class));
//                System.out.println(message.getBody());
                if(set.contains(message.getTransactionId())) System.out.println("---!!!!---"+message.getTransactionId());
                set.add(message.getTransactionId());
            }
            System.out.println("total is "+total + ", cost time " + (System.currentTimeMillis() - start));
            System.out.println("next" + x.getNextOffset());
            System.out.println("tsId count " + set.size());
            TimeUnit.MILLISECONDS.sleep(10);
//            break;
        }
    }

    private MessageListVO queryMessage(ShinanoProducerClient shinanoProducerClient,
                                       long offset,
                                       int count) throws InterruptedException {
        RemotingCommand request = new RemotingCommand();
        request.setFlag(RemotingCommandFlagConstants.TOPIC_INFO_QUERY);

        request.addExtField(ExtFieldsConstants.TOPIC_QUERY_OPT_KEY, TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE);
        request.addExtField(ExtFieldsConstants.QUERY_TOPIC_MESSAGE_COUNT_KEY, String.valueOf(count));

        request.setTransactionId(UUID.randomUUID().toString());
        request.addExtField(ExtFieldsConstants.TOPIC_KEY, "test-create1");
        request.addExtField(ExtFieldsConstants.QUEUE_KEY, "queue1");
        request.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));

        final MessageListVO[] res = new MessageListVO[1];

        CountDownLatch latch = new CountDownLatch(1);


        shinanoProducerClient.sendMessage(request, remotingCommand->{
            byte[] body = remotingCommand.getBody();
            byte[] array = ByteBuffer.wrap(body).array();
            res[0] = ProtostuffUtils.deserialize(array, MessageListVO.class);
            System.out.println("-------------");
            latch.countDown();
        });

        latch.await();
        return res[0];
    }

}
