package cn.com.shinano.ShinanoMQ.producer;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;

public interface ProducerResultCallback {

    abstract public void success(RemotingCommand response);

    abstract public void fail(RemotingCommand response);
}
