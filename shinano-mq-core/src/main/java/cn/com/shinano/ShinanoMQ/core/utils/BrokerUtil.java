package cn.com.shinano.ShinanoMQ.core.utils;

import cn.com.shinano.ShinanoMQ.core.config.SystemConfig;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class BrokerUtil {
    public static long getBrokerMessageId() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    public static String makeTopicQueueKey(String topic, String queue) {
        return topic + "-" + queue;
    }

    public static String getTopicQueueSaveDir(String topic, String queue) {
        return SystemConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue;
    }

    public static String getSaveFileName(Long startOffset) {
        return String.format("%020d", startOffset);
    }

    public static long getOffsetFromFile(String topic, String queue) {
        File dirFile = new File(SystemConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue);
        if (!dirFile.exists()) return -1;//没有topic-queue，-1表示没有初始化

        File[] dataLogs = dirFile.listFiles();

        if (dataLogs == null || dataLogs.length == 0) {//没有topic-queue，-1表示没有初始化
            return -1;
        } else { //里面有历史消息

            //通过offset最大的文件计算当前offset大小
            File newest = getNewestPersistentFile(dataLogs);
            long fileLogicStart = Long.parseLong(newest.getName().split("\\.")[0]);
            long fileLength = newest.length();

            return fileLogicStart + fileLength;
        }
    }

    /**
     * 获取files中最新的那个文件
     * @param files 存储消息的持久化文件数组
     * @return files里最新的
     */
    public static File getNewestPersistentFile(File[] files) {
        List<File> sorted = Arrays.stream(files).sorted((f1, f2) -> {
            return f2.getName().compareTo(f1.getName());
        }).collect(Collectors.toList());

        //通过offset最大的文件计算当前offset大小
        return sorted.get(0);
    }
}
