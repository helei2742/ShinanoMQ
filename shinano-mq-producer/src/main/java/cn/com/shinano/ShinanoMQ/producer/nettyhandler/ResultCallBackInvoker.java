package cn.com.shinano.ShinanoMQ.producer.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MsgFlagConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
@Slf4j
public abstract class ResultCallBackInvoker {

    private ConcurrentMap<String, Consumer<Message>> successCallbackMap;
    private ConcurrentMap<String, Consumer<Message>> failCallbackMap;


    public void init() {
        this.successCallbackMap = new ConcurrentHashMap<>();
        this.failCallbackMap = new ConcurrentHashMap<>();
    }

    /**
     * 添加消息的回调
     * @param transactionId 消息的事务id
     * @param successCallback      成功回调
     */
    public void addAckListener(String transactionId, Consumer<Message> successCallback) {
        this.successCallbackMap.put(transactionId, successCallback);

    }
    /**
     * 添加消息的回调
     * @param transactionId 消息的事务id
     * @param success      成功回调
     * @param fail      失败回调
     */
    public void addAckListener(String transactionId, Consumer<Message> success, Consumer<Message> fail) {
        this.successCallbackMap.put(transactionId, success);
        this.failCallbackMap.put(transactionId, fail);
    }

    /**
     * 执行回调
     * @param transactionId 消息的事务id
     * @param msg broker返回的消息
     */
    public void invokeCallBack(String transactionId, Message msg) {
        boolean flag = msg.getProperties() != null && msg.getProperties().containsKey(MsgFlagConstants.REQUEST_ERROR);

//        log.debug("get ack of message transactionId[{}], ack[{}], msg[{}]", transactionId, flag, msg);

        Consumer<Message> success = successCallbackMap.remove(transactionId);
        Consumer<Message> fail = failCallbackMap.remove(transactionId);
        if(!flag) {
            if (success != null) {
                success.accept(msg);
            }
        }else {
            if (fail != null) {
                fail.accept(msg);
            }
        }
    }

    /**
     * 执行成功回调
     * @param transactionId 消息的事务id
     * @param msg broker返回的消息
     */
    public void invokeSuccessCallBack(String transactionId, Message msg) {
        Consumer<Message> success = successCallbackMap.remove(transactionId);
        if (success != null) {
            success.accept(msg);
        }
    }
    /**
     * 执行成功回调
     * @param transactionId 消息的事务id
     * @param msg broker返回的消息
     */
    public void invokeFailCallBack(String transactionId, Message msg) {
        Consumer<Message> fail = failCallbackMap.remove(transactionId);
        if (fail != null) {
            fail.accept(msg);
        }
    }
}
