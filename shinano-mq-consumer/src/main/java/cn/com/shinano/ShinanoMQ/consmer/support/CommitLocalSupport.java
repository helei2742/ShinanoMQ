package cn.com.shinano.ShinanoMQ.consmer.support;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.consmer.config.ConsumerConfig;
import cn.com.shinano.ShinanoMQ.consmer.dto.LocalCommitLog;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 提交offset到remote失败后的降级策略，提交到本地文件
 * @author lhe.shinano
 * @date 2023/11/20
 */
@Slf4j
public class CommitLocalSupport {

    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    private static volatile BufferedWriter COMMIT_LOG_WRITER = null;

    private static final Timer timer = new Timer(true);

    /**
     * 提交到本地
     * @param command
     */
    public static void commitLocal(RemotingCommand command) {
        executor.submit(()->{
            if (COMMIT_LOG_WRITER == null) {
                synchronized(CommitLocalSupport.class) {
                    if(COMMIT_LOG_WRITER == null) {
                        try {
                            COMMIT_LOG_WRITER = new BufferedWriter(new FileWriter(ConsumerConfig.LOCAL_COMMIT_LOCATION, true));
                        } catch (IOException e) {
                            log.error("create commit log file error", e);
                        }
                    }
                }
            }

            LocalCommitLog commitLog = new LocalCommitLog(command);
            try {
                synchronized (CommitLocalSupport.class) {
                    COMMIT_LOG_WRITER.write(JSON.toJSONString(commitLog));
                    COMMIT_LOG_WRITER.newLine();
                }
                log.info("push batch consume offset to local file, [{}]", commitLog);
            } catch (IOException e) {
                log.error("commit local fail", e);
            }
        });
    }


}
