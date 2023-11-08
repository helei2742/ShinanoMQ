package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.base.Message;
import cn.com.shinano.ShinanoMQ.base.MessageOPT;
import cn.com.shinano.ShinanoMQ.base.SaveMessage;
import cn.com.shinano.ShinanoMQ.core.dto.IndexNode;
import cn.com.shinano.ShinanoMQ.core.service.AbstractBrokerService;
import cn.com.shinano.ShinanoMQ.core.service.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.service.TopicQueryService;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.hutool.core.lang.Pair;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;


@Slf4j
@Service
public class TopicQueryServiceImpl extends AbstractBrokerService implements TopicQueryService {

    @Autowired
    private OffsetManager offsetManager;

    /**
     * 查询topic下queue中消息当前的offset
     * @param message 请求体
     * @param channel 数据返回的channel
     */
    @Override
    public void queryTopicQueueOffset(Message message, Channel channel) {
        long l = offsetManager.queryTopicQueueOffset(message.getTopic(), message.getQueue());
        sendMessage(MessageOPT.TOPIC_INFO_QUERY_RESULT, String.valueOf(l), channel);
    }

    /**
     * 查询topic下queue中 offset 位置后的消息
     * @param message 请求体
     * @param channel 数据返回的channel
     */
    @Override
    public void queryTopicQueueOffsetMsg(Message message, Channel channel) {

        try {
            Pair<List<SaveMessage>, Long> listLongPair = queryTopicQueueAfterOffsetMsg(
                    message.getTopic(),
                    message.getQueue(),
                    Long.parseLong(new String(message.getBody(),StandardCharsets.UTF_8)));

            message.setFlag(MessageOPT.TOPIC_INFO_QUERY_RESULT);

            message.setBody(JSON.toJSONString(listLongPair).getBytes(StandardCharsets.UTF_8));

        } catch (IOException e) {
            log.error("query message[{}] get error", message, e);
        }

        sendMessage(message, channel);
    }


    /**
     * 查询topic queue 中 在offset之后的消息, 最多只会返回一个数据文件大小的内容。
     * @param topic topic name
     * @param queue queue name
     * @param logicOffset logicOffset
     * @return
     */
    @Override
    public Pair<List<SaveMessage>, Long> queryTopicQueueAfterOffsetMsg(String topic, String queue, Long logicOffset) throws IOException {
        Path path = Paths.get(BrokerUtil.getTopicQueueSaveDir(topic, queue));
        if(!Files.exists(path)) return null;

        //获取数据文件里offset的插入点文件名
        String filename = getIndexFileNameWithoutFix(path, logicOffset);

        Path indexPath = Paths.get(path.toAbsolutePath().toString(),filename+".idx");
        Path dataPath = Paths.get(path.toAbsolutePath().toString(),filename+".dat");

        List<SaveMessage> list = null;
        long startOffset = Long.parseLong(filename);
        if(!Files.exists(indexPath)) { //没有索引文件,直接读数据文件

            list = readDataFileAfterOffset(dataPath.toFile(), 0, logicOffset - startOffset, 10);
        } else { //有索引

            //读取索引文件
            List<IndexNode> indexList = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(indexPath.toFile()))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    indexList.add(IndexNode.toIndexNode(line));
                }
            }

            //二分找在index的位置
            if (indexList.size() == 0) throw new IllegalArgumentException("index file context is empty");
            int i = Collections.binarySearch(indexList, new IndexNode(logicOffset, 0L));
            if (i < 0) {
                i = Math.abs(i) - 2;
            }

            if(i < 0) { // 没有比当前小offset的索引
                list = readDataFileAfterOffset(dataPath.toFile(), 0, logicOffset - startOffset, 10);
            }else {
                Long fileOffset = indexList.get(i).getFileOffset();
                list = readDataFileAfterOffset(dataPath.toFile(), fileOffset, logicOffset - startOffset, 10);
            }
        }

        long next = Long.parseLong(filename) + dataPath.toFile().length();
        return new Pair<>(list, next);
    }


    /**
     * 读取数据文件，从fileOffset开始向后找，找到targetOffset处后的放到结果集中
     * @param file  文件
     * @param fileOffset   读取文件的起始物理偏移
     * @param targetOffset 读取文件目标物理偏移
     * @return  数据文件的消息列表
     * @throws IOException
     */
    private List<SaveMessage> readDataFileAfterOffset(File file, long fileOffset, long targetOffset, int count) throws IOException {
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, fileOffset, file.length()-fileOffset);

        byte[] lengthBytes = new byte[8];


        List<SaveMessage> messages = new ArrayList<>();

        while (map.position() < map.capacity()) {
            map.get(lengthBytes);

            int length = ByteBuffer.wrap(lengthBytes).getInt();
            if (length == 0) break;

            byte[] msgBytes = new byte[length];
            map.get(msgBytes);
            if(map.position()-8-length + fileOffset >= targetOffset) {
                String json = new String(msgBytes, StandardCharsets.UTF_8);
                messages.add(JSONObject.parseObject(json, SaveMessage.class));

                if(messages.size() >= count) break;
            }
        }
        return messages;
    }

    /**
     * 获取 logicOffset 这个逻辑偏移 在哪个文件之后
     * @param path          数据文件的文件夹
     * @param logicOffset   逻辑偏移
     * @return              没有后缀的文件名
     * @throws IOException
     */
    private String getIndexFileNameWithoutFix(Path path, long logicOffset) throws IOException {

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

        return BrokerUtil.getSaveFileName(startOffsets.get(index));
    }
}
