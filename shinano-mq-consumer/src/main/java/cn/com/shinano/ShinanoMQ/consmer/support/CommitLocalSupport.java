package cn.com.shinano.ShinanoMQ.consmer.support;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.consmer.ShinanoConsumerClient;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.dto.LocalCommitLog;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * 提交offset到remote失败后的降级策略，提交到本地文件
 * @author lhe.shinano
 * @date 2023/11/20
 */
@Slf4j
public class CommitLocalSupport {

    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    private static volatile BufferedWriter COMMIT_LOG_WRITER = null;

    private final ShinanoConsumerClient consumerClient;

    private final PriorityBlockingQueue<LocalCommitLog> queue;


    public CommitLocalSupport(ShinanoConsumerClient consumerClient) {
        this.consumerClient = consumerClient;
        this.queue = new PriorityBlockingQueue<LocalCommitLog>(1000,
                (l1,l2)->Long.compare(l1.getLastRetryTimestamp(), l2.getLastRetryTimestamp()));

        executor.execute(()->{
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    LocalCommitLog commitLog = queue.take();

                    if(System.currentTimeMillis() - commitLog.getLastRetryTimestamp() >=
                            ConsumerConfig.COMMIT_REMOTE_RETRY_COUNT[commitLog.getRetryCount()]){
                        boolean f = true;

                        try {
                            f = consumerClient.sendMsg(commitLog.getCommand());
                        } catch (InterruptedException e) {
                            log.error("retry to commit consume offset error",e);
                            f = false;
                        }

                        if(!f) {
                            commitLog.incrementRetryCount();
                            commitLog.setLastRetryTimestamp(System.currentTimeMillis());

                            if(commitLog.getRetryCount() >= ConsumerConfig.COMMIT_REMOTE_RETRY_COUNT.length) { //超过限制，存入文件
                                log.warn("retry commit consume offset fail, tsId[{}] retry count [{}] out of limit, save into local file",
                                        commitLog.getCommand().getTransactionId(),commitLog.getRetryCount());
                                saveInLocalFile(commitLog);
                            } else { //继续重试
                                log.warn("retry commit consume offset fail, tsId[{}] retry count [{}]",
                                        commitLog.getCommand().getTransactionId(),commitLog.getRetryCount());
                                queue.offer(commitLog);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    log.warn("retry commit consume offset thread interrupted, save uncompleted into local, total[{}]", queue.size());
                    while (!queue.isEmpty()) {
                        LocalCommitLog poll = queue.poll();
                        saveInLocalFile(poll);
                    }
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /**
     * 提交到本地
     * @param command
     */
    public void commitLocal(RemotingCommand command) {
        LocalCommitLog commitLog = new LocalCommitLog(command);
        queue.offer(commitLog);
    }


    private synchronized void saveInLocalFile(LocalCommitLog commitLog) {
        if (COMMIT_LOG_WRITER == null) {
            try {
                File file = new File(ConsumerConfig.LOCAL_COMMIT_LOCATION);
                if(!file.exists()) file.getParentFile().mkdirs();
                COMMIT_LOG_WRITER = new BufferedWriter(new FileWriter(file, true));
            } catch (IOException e) {
                log.error("create commit log file error", e);
            }
        }
        try {
            COMMIT_LOG_WRITER.write(JSON.toJSONString(commitLog));
            COMMIT_LOG_WRITER.newLine();
            COMMIT_LOG_WRITER.flush();
            log.info("push batch consume offset to local file, [{}]", commitLog);
        } catch (IOException e) {
            log.error("commit local fail", e);
        }
    }
}
