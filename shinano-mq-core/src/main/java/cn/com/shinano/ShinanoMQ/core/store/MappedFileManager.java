package cn.com.shinano.ShinanoMQ.core.store;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.config.BrokerSpringConfig;
import cn.com.shinano.ShinanoMQ.core.manager.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/12/6
 */
@Slf4j
@Component
public class MappedFileManager {

    private static final long MAPPED_FILE_EXPIRE_TIME = 3 * 60 * 1000;

    private final ConcurrentMap<String, MappedFile> mappedFileCache = new ConcurrentHashMap<>();

    private final HashMap<String, Long> mappedFileExpireMap = new HashMap<>();


    @Autowired
    private OffsetManager offsetManager;

    @Autowired
    private BrokerSpringConfig springConfig;

    /**
     * 获取MappedFile， 如果不存在，会创建一个
     * @param topic topic
     * @param queue queue
     * @param offset offset 该文件的offset，会安装BrokerConfig.PERSISTENT_FILE_SIZE 也就是数据文件大小的整数倍来取
     * @return MappedFile
     */
    public MappedFile getMappedFile(String topic, String queue, long offset) {
        int index = (int) (offset / BrokerConfig.PERSISTENT_FILE_SIZE);
        String key = generateKey(topic, queue, index);
        mappedFileCache.compute(key, (k, v) -> {
            if (v == null) {
                v = createNewMappedFile(topic, queue, BrokerConfig.PERSISTENT_FILE_SIZE * index, index);
            }
            mappedFileExpireMap.put(k, System.currentTimeMillis() + MAPPED_FILE_EXPIRE_TIME);
            return v;
        });
        return mappedFileCache.get(key);
    }

    /**
     * 获取MappedFile， 如果不存在，返回null
     * @param topic topic
     * @param queue queue
     * @param offset offset 该文件的offset，会安装BrokerConfig.PERSISTENT_FILE_SIZE 也就是数据文件大小的整数倍来取
     * @return MappedFile
     */
    public MappedFile getMappedFileNoCreate(String topic, String queue, long offset) {
        int index = (int) (offset / BrokerConfig.PERSISTENT_FILE_SIZE);
        String key = generateKey(topic, queue, index);
        return mappedFileCache.get(key);
    }

    /**
     * 新建一个MappedFile
     * @param topic topic
     * @param queue queue
     * @param offset offset
     * @param index index  第几个数据文件
     * @return MappedFile
     */
    private MappedFile createNewMappedFile(String topic, String queue, long offset, int index) {
        File dirFile = new File(BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue);

        if (!dirFile.exists() && !dirFile.mkdirs()) {
            throw new RuntimeException("dir create fail, " + dirFile.toString());
        }

        File dataFile = StoreFileUtil.getDataFileOfLogicOffset(topic, queue, offset);
        MappedFile mappedFile = null;
        long currentWrite;
        try {
            //该数据文件存在,并且是当前写的文件
            if (dataFile.exists()
                    && (currentWrite = offsetManager.queryTopicQueueOffset(topic, queue)) != -1
                    && currentWrite / BrokerConfig.PERSISTENT_FILE_SIZE == index) {
                mappedFile = new MappedFile(offset, (int) (currentWrite - BrokerConfig.PERSISTENT_FILE_SIZE * index),
                        BrokerConfig.PERSISTENT_FILE_SIZE, dataFile);
            } else {
                mappedFile = new MappedFile(offset, 0, BrokerConfig.PERSISTENT_FILE_SIZE, dataFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mappedFile;
    }

    /**
     * 生成存入mappedFileCache以及mappedFileExpireMap的key
     * @param topic topic
     * @param queue queue
     * @param index index
     * @return key string
     */
    private String generateKey(String topic, String queue, int index) {
        return topic + BrokerUtil.KEY_SEPARATOR + queue + BrokerUtil.KEY_SEPARATOR + index;
    }


    /**
     * 将已存在的MappedFile内容刷盘，如果该MappedFile超过时间未使用，则会将其移除
     * @throws IOException IOException
     */
    public void flushMappedFiles() throws IOException {
        for (Map.Entry<String, MappedFile> entry : mappedFileCache.entrySet()) {
            MappedFile mappedFile = entry.getValue();

            if (mappedFile == null) continue;

            if (System.currentTimeMillis() - mappedFile.getLastFlushTime() >= springConfig.getMappedFileFlushInterval()) {
                log.debug("flush mappedFile [{}]", mappedFile.getFilename());
                mappedFile.flush();
            }

            mappedFileCache.computeIfPresent(entry.getKey(), (k, v) -> {
                if (System.currentTimeMillis() >= mappedFileExpireMap.get(k)) {
                    log.warn("mapped file [{}] expire, release it", v.getFilename());
                    mappedFileExpireMap.remove(k);
                    return null;
                }
                return v;
            });
        }
    }
}
