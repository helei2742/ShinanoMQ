package cn.com.shinano.ShinanoMQ.producer.spring;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.producer.ProducerResultCallback;

public class DefaultResultCallback implements ProducerResultCallback {
    @Override
    public void success(RemotingCommand response) {
        System.out.println("-------success------");
        System.out.println(response);
    }

    @Override
    public void fail(RemotingCommand response) {
        System.out.println("-------fail------");
        System.out.println(response);
    }
}
