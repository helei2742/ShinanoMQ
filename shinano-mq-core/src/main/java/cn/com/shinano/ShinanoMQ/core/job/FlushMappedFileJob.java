package cn.com.shinano.ShinanoMQ.core.job;

import cn.com.shinano.ShinanoMQ.core.datalog.MappedFile;
import cn.com.shinano.ShinanoMQ.core.service.impl.MappedChannelPersistentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/9
 */
@Slf4j
@Component
public class FlushMappedFileJob {

    @Autowired
    private MappedChannelPersistentService persistentService;

    @Scheduled(cron = "0/10 * * * * *")
    public void flushMappedFile() {
        Map<String, MappedChannelPersistentService.PersistentTask> map = persistentService.getPersistentTask();

        log.info("start flush mapped file");
        for (String key : map.keySet()) {
            MappedFile mappedFile = map.get(key).getMappedFile();
            if(mappedFile == null) continue;

            if(System.currentTimeMillis() - mappedFile.getLastFlushTime() >= 10000) {
                try {
                    mappedFile.flush();
                } catch (IOException e) {
                    log.error("flush mappedFile get an error", e);
                }
            }
        }
    }
}

