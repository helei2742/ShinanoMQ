package cn.com.shinano.ShinanoMQ.core.job;

import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.store.MappedFile;
import cn.com.shinano.ShinanoMQ.core.manager.impl.MappedChannelPersistentManager;
import cn.com.shinano.ShinanoMQ.core.store.MappedFileManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.Time;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
@Slf4j
@Component
public class FlushMappedFileJob implements InitializingBean {

    private Thread flushMappedFileThread;

    @Autowired
    private MappedChannelPersistentManager persistentService;

    @Autowired
    private MappedFileManager mappedFileManager;

    @Autowired
    private BrokerSpringConfig brokerSpringConfig;

    //    @Scheduled(cron = "0/10 * * * * *")
    @Deprecated
    public void flushMappedFile2() {
        Map<String, MappedChannelPersistentManager.PersistentTask> map = persistentService.getPersistentTask();

        log.debug("start flush mapped file");
        for (String key : map.keySet()) {
            MappedFile mappedFile = map.get(key).getMappedFile();
            if (mappedFile == null) continue;

            if (System.currentTimeMillis() - mappedFile.getLastFlushTime() >= 10000) {
                try {
                    mappedFile.flush();
                    System.out.println("------total append" + mappedFile.counter.get());
                } catch (IOException e) {
                    log.error("flush mappedFile get an error", e);
                }
            }
        }
    }

    public void flushMappedFile() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                log.debug("start flush mapped file");
                mappedFileManager.flushMappedFiles();

                TimeUnit.MILLISECONDS.sleep(brokerSpringConfig.getMappedFileFlushInterval());
            } catch (InterruptedException e) {
                log.warn("flush mapped file thread interrupted, exit now");
            } catch(IOException e){
                log.error("flush mappedFile get an error", e);
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        flushMappedFileThread = new Thread(this::flushMappedFile, "flushMappedFileThread");
        flushMappedFileThread.start();
    }
}

