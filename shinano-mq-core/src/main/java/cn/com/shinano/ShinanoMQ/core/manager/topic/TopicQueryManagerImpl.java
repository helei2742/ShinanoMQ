package cn.com.shinano.ShinanoMQ.core.manager.topic;

import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.base.dto.MsgFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.dto.IndexNode;
import cn.com.shinano.ShinanoMQ.core.manager.AbstractBrokerManager;
import cn.com.shinano.ShinanoMQ.core.manager.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.TopicQueryManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
@Service
public class TopicQueryManagerImpl extends AbstractBrokerManager implements TopicQueryManager {

    private static final ExecutorService executor = Executors.newFixedThreadPool(4);

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
        sendMessage(MsgFlagConstants.TOPIC_INFO_QUERY_RESULT, String.valueOf(l), channel);
    }

    /**
     * 查询topic下queue中 offset 位置后的消息
     * @param message 请求体
     * @param channel 数据返回的channel
     */
    @Override
    public void queryTopicQueueOffsetMsg(Message message, int count, Channel channel) {

        executor.execute(()->{
            try {
                MessageListVO vo = queryTopicQueueAfterOffsetMsg(
                        message.getTopic(),
                        message.getQueue(),
                        Long.parseLong(new String(message.getBody(),StandardCharsets.UTF_8)),
                        count);

                message.setFlag(MsgFlagConstants.TOPIC_INFO_QUERY_RESULT);
                HashMap<String, String> map = new HashMap<>();

                message.setProperties(map);

                message.setBody(ProtostuffUtils.serialize(vo));
            } catch (IOException e) {
                log.error("query message[{}] get error", message, e);
            }

            sendMessage(message, channel);
        });
    }


    /**
     * 查询topic queue 中 在offset之后的消息
     * @param topic topic name
     * @param queue queue name
     * @param logicOffset logicOffset
     * @param count 从logicOffset开始查几条
     * @return
     */
    public MessageListVO queryTopicQueueAfterOffsetMsg(String topic,
                                                       String queue,
                                                       Long logicOffset,
                                                       int count) throws IOException {

        Path path = Paths.get(BrokerUtil.getTopicQueueSaveDir(topic, queue));
        if(!Files.exists(path)) return new MessageListVO();

        //获取数据文件里offset的插入点文件名
        String filename = getIndexFileNameWithoutFix(path, logicOffset);

        //跨文件获取
        MessageListVO res = null;
        MessageListVO temp = null;
        int curNeed = count;
        long curLogicNeed = logicOffset;
        while (curNeed != 0) {
            temp = getMessageListVO(curLogicNeed, curNeed, path, filename);

            if(res == null) res = temp;
            else {
                res.getMessages().addAll(temp.getMessages());
                res.setNextOffset(temp.getNextOffset());
            }

            curNeed -= temp.getMessages().size();
            curLogicNeed += temp.getNextOffset();

            if(curNeed == 0) break; //获取完

            String nextFileName = getIndexFileNameWithoutFix(path, curLogicNeed);
            if(nextFileName.equals(filename)) break; //没有下一个数据文件
            filename = nextFileName;
        }

        return res;
    }

    private MessageListVO getMessageListVO(Long logicOffset, int count, Path path, String filename) throws IOException {
        Path indexPath = Paths.get(path.toAbsolutePath().toString(), filename +".idx");
        Path dataPath = Paths.get(path.toAbsolutePath().toString(), filename +".dat");

        MessageListVO res = null;
        long startOffset = Long.parseLong(filename);
        if(!Files.exists(indexPath)) { //没有索引文件,直接读数据文件

            res = readDataFileAfterOffset(dataPath.toFile(), 0, logicOffset - startOffset, startOffset, count);
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
                i = Math.abs(i) - 1;
            }

            File file = dataPath.toFile();
            if(i == 0) { // 没有比当前小offset的索引
                res = readDataFileAfterOffset(file, 0, logicOffset - startOffset, startOffset, count);
            }else {
                i = Math.min(i, indexList.size()-1);
                IndexNode indexNode = indexList.get(i);
                if(indexNode.getLogicOffset() > logicOffset) {
                    indexNode = indexList.get(Math.max(0, i-1));
                }
                long fileOffset = indexNode.getFileOffset();
                long targetOffset = logicOffset - startOffset;


                res = readDataFileAfterOffset(file, fileOffset, targetOffset, logicOffset, count);
            }
        }
        return res;
    }


    /**
     * 读取数据文件，从fileOffset开始向后找，找到targetOffset处后的放到结果集中
     * @param file  文件
     * @param fileOffset   读取文件的起始物理偏移
     * @param targetOffset 读取文件目标物理偏移
     * @return  数据文件的消息列表
     * @throws IOException
     */
    private MessageListVO readDataFileAfterOffset(File file,
                                                  long fileOffset,
                                                  long targetOffset,
                                                  long startOffset,
                                                  int count) throws IOException {

        System.out.printf("fileOffset %d, targetOffset %d, startOffset %d\n", fileOffset, targetOffset, startOffset);
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, fileOffset, file.length()-fileOffset);

        byte[] lengthBytes = new byte[8];


        List<SaveMessage> messages = new ArrayList<>();

        while (map.position() < map.capacity()) {
            if(map.remaining() < 8) break;
            map.get(lengthBytes);

            int length = ByteBuffer.wrap(lengthBytes).getInt();
            if (length == 0) break;

            byte[] msgBytes = new byte[length];
            map.get(msgBytes);
            if(map.position()+fileOffset > targetOffset) {
//                String json = new String(msgBytes, StandardCharsets.UTF_8);
//                messages.add(JSONObject.parseObject(json, SaveMessage.class));
                messages.add(BrokerUtil.brokerSaveBytesTurnMessage(msgBytes));
                if(messages.size() >= count) break;
            }
        }

        MessageListVO vo = new MessageListVO();
        vo.setMessages(messages);
        vo.setNextOffset((long) map.position());
        return vo;
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

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(6);
        list.add(8);
        list.add(10);
        System.out.println(Collections.binarySearch(list, 1));
        System.out.println(Collections.binarySearch(list, 5));
        System.out.println(Collections.binarySearch(list, 7));
        System.out.println(Collections.binarySearch(list, 8));
        System.out.println(Collections.binarySearch(list, 11));
    }
}
