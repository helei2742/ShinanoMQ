package cn.com.shinano.ShinanoMQ.core.store;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.manager.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/12/6
 */
@Slf4j
@Component
public class MappedFileManager {


    private final ConcurrentMap<String, MappedFile> mappedFileCache = new ConcurrentHashMap<>();

    @Autowired
    private OffsetManager offsetManager;


    public MappedFile getMappedFile(String topic, String queue, long offset) {
        int index = (int) (offset / BrokerConfig.PERSISTENT_FILE_SIZE);
        String key = generateKey(topic, queue, index);
        mappedFileCache.compute(key, (k,v)->{
            if (v == null) {
                v = createNewMappedFile(topic, queue, BrokerConfig.PERSISTENT_FILE_SIZE*index, index);
            }
            return v;
        });
        return mappedFileCache.get(key);
    }

    public MappedFile createNewMappedFile(String topic, String queue, long offset, int index) {
        File dirFile = new File(BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue);

        if (!dirFile.exists() && !dirFile.mkdirs()) {
            throw new RuntimeException("dir create fail, " + dirFile.toString());
        }

        File dataFile = StoreFileUtil.getDataFileOfLogicOffset(topic, queue, offset);
        MappedFile mappedFile = null;
        long currentWrite;
        try {
            if (dataFile.exists() && (currentWrite=offsetManager.queryTopicQueueOffset(topic, queue)) != -1) { //该数据文件存在,并且有插入数据
                mappedFile = new MappedFile(currentWrite, (int) (currentWrite-BrokerConfig.PERSISTENT_FILE_SIZE*index),
                        BrokerConfig.PERSISTENT_FILE_SIZE, dataFile);
            } else {
                mappedFile = new MappedFile(offset, 0, BrokerConfig.PERSISTENT_FILE_SIZE, dataFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mappedFile;
    }

    private String generateKey(String topic, String queue, int index) {
        return topic + BrokerUtil.KEY_SEPARATOR + queue + BrokerUtil.KEY_SEPARATOR + index;
    }
}
