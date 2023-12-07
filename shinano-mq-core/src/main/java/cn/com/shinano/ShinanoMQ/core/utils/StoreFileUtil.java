package cn.com.shinano.ShinanoMQ.core.utils;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.hutool.core.util.RandomUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
public class StoreFileUtil {
    public static String getTopicQueueSaveDir(String topic, String queue) {
        return BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue;
    }

    public static String getSaveFileName(Long startOffset) {
        return String.format("%020d", startOffset);
    }

    /**
     * 获取topic下queue的offset
     * @param topic
     * @param queue
     * @return
     */
    public static long getOffsetFromFile(String topic, String queue) {
        File dirFile = new File(BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue);
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
        List<File> sorted = Arrays.stream(files).filter(f->f.getName().endsWith(".dat")).sorted((f1, f2) -> {
            return f2.getName().compareTo(f1.getName());
        }).collect(Collectors.toList());

        //通过offset最大的文件计算当前offset大小
        return sorted.get(0);
    }
    /**
     * 获取offset应该存储文件的文件名
     * @param topic
     * @param queue
     * @param offset
     * @return
     */
    public static File getDataFileOfLogicOffset(String topic, String queue, long offset){
        long index = (offset / BrokerConfig.PERSISTENT_FILE_SIZE);
        Path path = Paths.get(StoreFileUtil.getTopicQueueSaveDir(topic, queue));
        long filename = BrokerConfig.PERSISTENT_FILE_SIZE * index;

        return new File(path.toString() + File.separator + getSaveFileName(filename) + ".dat");
    }

    /**
     * 获取 logicOffset 这个逻辑偏移 在哪个文件之后
     * @param path          数据文件的文件夹
     * @param logicOffset   逻辑偏移
     * @return              没有后缀的文件名
     * @throws IOException
     */
    @Deprecated
    public static String getIndexFileNameWithoutFix(Path path, long logicOffset) throws IOException {

        List<Long> startOffsets = new ArrayList<>();
        Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if(file.toString().endsWith(".dat")) { //数据文件
                    String str = file.getName(file.getNameCount() - 1).toString().replace(".dat", "");
                    startOffsets.add(Long.parseLong(str));
                }
                return FileVisitResult.SKIP_SUBTREE;
            }
        });

        if(startOffsets.size() == 0) throw new FileNotFoundException(path + " didn't have data file");

        startOffsets.sort(Long::compareTo);
        int index = Collections.binarySearch(startOffsets, logicOffset);

        if(index < 0)
            index = Math.abs(index) - 2;

        return StoreFileUtil.getSaveFileName(startOffsets.get(index));
    }

//    public static long getLogicOffsetSaveFileName(long logicOffset) {
//
//    }

    /**
     * 移动topic的数据文件，
     * @param topic
     */
    public static void moveTopicData(String topic) {
        String dir = BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic;
        new File(dir)
                .renameTo(new File(dir +"_deleted_" + RandomUtil.randomNumbers(8)));
    }

    public static File getRetryLogSaveFile(String clientId, String topic, String queue) {
        File dir = new File(BrokerConfig.RETRY_LOG_SAVE_DIR);
        if(!dir.exists()) dir.mkdirs();

        return new File(dir.toString() + File.separator +
                String.format(BrokerConfig.RETRY_LOG_SAVE_FILE_PATTERN, clientId, topic, queue));
    }
}
