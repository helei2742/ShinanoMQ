package cn.com.shinano.ShinanoMQ.base;

import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
@Slf4j
public abstract class ResultCallBackInvoker {

    private long expireSeconds = ShinanoMQConstants.BROKER_RESPONSE_TIME_LIMIT;

    private ConcurrentMap<String, Consumer<RemotingCommand>> successCallbackMap;
    private ConcurrentMap<String, Consumer<RemotingCommand>> failCallbackMap;
    private ConcurrentMap<String, Long> expireMap;
    private Timer timer;

    public void init() {
        this.successCallbackMap = new ConcurrentHashMap<>();
        this.failCallbackMap = new ConcurrentHashMap<>();
        this.expireMap = new ConcurrentHashMap<>();

        this.timer = new Timer("expireClientHandlerClear");
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                for (Map.Entry<String, Long> entry : expireMap.entrySet()) {
                    if (System.currentTimeMillis() > entry.getValue()) {
                        String tsId = entry.getKey();
                        successCallbackMap.remove(tsId);
                        Consumer<RemotingCommand> fail = failCallbackMap.remove(tsId);
                        if(fail != null) {
                            log.debug("handler expire, invoke fail handler, tsId[{}]", tsId);
                            fail.accept(RemotingCommand.TIME_OUT_COMMAND);
                        }
                    }
                }
            }
        }, ShinanoMQConstants.EXPIRE_CLIENT_HANDLER_CLEAR_INTERVAL, ShinanoMQConstants.EXPIRE_CLIENT_HANDLER_CLEAR_INTERVAL);
    }

    public void setExpireSeconds(int seconds) {
        this.expireSeconds = seconds * 1000L;
    }

    /**
     * 添加消息的回调
     * @param transactionId 消息的事务id
     * @param success      成功回调
     */
    public void addAckListener(String transactionId, Consumer<RemotingCommand> success) {
        if(success != null) this.successCallbackMap.put(transactionId, success);
    }

    /**
     * 添加消息的回调
     * @param transactionId 消息的事务id
     * @param success      成功回调
     * @param fail      失败回调
     */
    public void addAckListener(String transactionId, Consumer<RemotingCommand> success, Consumer<RemotingCommand> fail) {
        if(success != null) this.successCallbackMap.put(transactionId, success);
        if(fail != null) this.failCallbackMap.put(transactionId, fail);
        if(success !=null || fail != null)
            expireMap.put(transactionId, System.currentTimeMillis() + expireSeconds);
    }

    /**
     * 执行回调
     * @param transactionId 消息的事务id
     * @param remotingCommand broker返回的消息
     */
    public void invokeCallBack(String transactionId, RemotingCommand remotingCommand) {
        log.debug("invoke call back, transactionId[{}]", transactionId);
        Consumer<RemotingCommand> success = successCallbackMap.remove(transactionId);
        Consumer<RemotingCommand> fail = failCallbackMap.remove(transactionId);
        if(!(RemotingCommandCodeConstants.FAIL == remotingCommand.getCode())) {
            if (success != null) {
                success.accept(remotingCommand);
            }
        }else {
            if (fail != null) {
                fail.accept(remotingCommand);
            }
        }
    }

    /**
     * 执行成功回调
     * @param transactionId 消息的事务id
     * @param remotingCommand broker返回的消息
     */
    public void invokeSuccessCallBack(String transactionId, RemotingCommand remotingCommand) {
        Consumer<RemotingCommand> success = successCallbackMap.remove(transactionId);
        if (success != null) {
            success.accept(remotingCommand);
        }
    }
    /**
     * 执行成功回调
     * @param transactionId 消息的事务id
     * @param remotingCommand broker返回的消息
     */
    public void invokeFailCallBack(String transactionId, RemotingCommand remotingCommand) {
        Consumer<RemotingCommand> fail = failCallbackMap.remove(transactionId);
        if (fail != null) {
            fail.accept(remotingCommand);
        }
    }

    public void resolveBatchACK(RemotingCommand remotingCommand){}
}
