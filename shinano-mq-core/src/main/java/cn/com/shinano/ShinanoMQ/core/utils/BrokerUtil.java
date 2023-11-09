package cn.com.shinano.ShinanoMQ.core.utils;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.hutool.core.util.RandomUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class BrokerUtil {

    public static String makeTopicQueueKey(String topic, String queue) {
        return topic + "-" + queue;
    }

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
        List<File> sorted = Arrays.stream(files).sorted((f1, f2) -> {
            return f2.getName().compareTo(f1.getName());
        }).collect(Collectors.toList());

        //通过offset最大的文件计算当前offset大小
        return sorted.get(0);
    }

    public static String getTransactionId(String transactionId) {
        if(transactionId == null) {
            transactionId = RandomUtil.randomString(8);
        }
        return UUID.nameUUIDFromBytes(transactionId.getBytes(StandardCharsets.UTF_8)).toString();
    }


    /**
     * 消息转换为存储的字节
     * @param message
     * @return
     */
    public static byte[] messageTurnBrokerSaveBytes(Message message) {
        SaveMessage saveMessage = new SaveMessage();
        saveMessage.setTransactionId(message.getTransactionId());
        saveMessage.setBody(message.getBody());
        saveMessage.setReconsumeTimes(0);
        saveMessage.setTimestamp(System.currentTimeMillis());
        saveMessage.setStoreHost(BrokerConfig.BROKER_HOST);

//        byte[] bytes = JSONObject.toJSONBytes(saveMessage);
        byte[] bytes = ProtostuffUtils.serialize(saveMessage);
        byte[] length = ByteBuffer.allocate(8).putInt(bytes.length).array();
        byte[] res = new byte[bytes.length + length.length];
        System.arraycopy(length, 0, res, 0, length.length);
        System.arraycopy(bytes, 0, res, length.length, bytes.length);
        return res;
    }

    public static SaveMessage brokerSaveBytesTurnMessage(byte[] msgBytes) {
        return ProtostuffUtils.deserialize(msgBytes, SaveMessage.class);
    }


    /**
     * 移动topic的数据文件，
     * @param topic
     */
    public static void moveTopicData(String topic) {
        String dir = BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic;
        new File(dir)
                .renameTo(new File(dir +"_deleted_" + RandomUtil.randomNumbers(8)));
    }
}
