package cn.com.shinano.ShinanoMQ.core.manager.topic;

import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.constans.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.config.TopicConfig;
import cn.com.shinano.ShinanoMQ.core.store.IndexNode;
import cn.com.shinano.ShinanoMQ.core.manager.AbstractBrokerManager;
import cn.com.shinano.ShinanoMQ.core.manager.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.TopicQueryManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.CompletableFuture;
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
     * @param topic
     * @param queue
     * @return
     */
    @Override
    public CompletableFuture<RemotingCommand> queryTopicQueueOffset(String topic, String queue) {
        long l = offsetManager.queryTopicQueueOffset(topic, queue);
        RemotingCommand remotingCommand = RemotingCommandPool.getObject();
        remotingCommand.setFlag(RemotingCommandFlagConstants.TOPIC_INFO_QUERY_RESULT);
        remotingCommand.addExtField(TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET, String.valueOf(l));
        return CompletableFuture.completedFuture(remotingCommand);
    }

    /**
     * 查询topic下queue中 offset 位置后的消息
     * @param topic
     * @param queue
     * @param offset
     * @param count
     * @return
     */
    @Override
    public CompletableFuture<RemotingCommand> queryTopicQueueOffsetMsg(String topic, String queue, long offset, int count) {

        return CompletableFuture.supplyAsync(()->{
            RemotingCommand remotingCommand = RemotingCommandPool.getObject();
            remotingCommand.setFlag(RemotingCommandFlagConstants.TOPIC_INFO_QUERY_RESULT);

            try {
                MessageListVO vo = queryTopicQueueAfterOffsetMsg(
                        topic,
                        queue,
                        offset,
                        count);
                remotingCommand.addExtField(TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE, String.valueOf(true));
                remotingCommand.setBody(ProtostuffUtils.serialize(vo));
                remotingCommand.setPayLoad(vo);
                return remotingCommand;
            } catch (IOException e) {
                log.error("query topic[{}] queue[{}] offset[{}] count[{}] get error", topic, offset, count, e);
            }
            remotingCommand.addExtField(TopicQueryConstants.QUERY_TOPIC_QUEUE_OFFSET_MESSAGE, String.valueOf(false));
            return remotingCommand;
        }, executor);
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

        //判断logicOffset是否超过当前写入的
        long offsetLimit = offsetManager.queryTopicQueueOffset(topic, queue);
        if (logicOffset >= offsetLimit) {
           return MessageListVO.empty();
        }


        Path path = Paths.get(StoreFileUtil.getTopicQueueSaveDir(topic, queue));
        if(!Files.exists(path)) return MessageListVO.empty();

        //获取数据文件里offset的插入点文件名
        String filename = StoreFileUtil.getIndexFileNameWithoutFix(path, logicOffset);

        //跨文件获取
        MessageListVO res = null;
        MessageListVO temp = null;
        int curNeed = count;
        long curLogicNeed = logicOffset;
        while (curNeed != 0) {
            temp = getMessageListVO(curLogicNeed, offsetLimit, curNeed, path, filename);

            if(res == null) res = temp;
            else {
                res.getMessages().addAll(temp.getMessages());
                res.setNextOffset(temp.getNextOffset());
            }

            curNeed -= temp.getMessages().size();
            curLogicNeed = temp.getNextOffset();

            if(curNeed == 0) break; //获取完

            String nextFileName = StoreFileUtil.getIndexFileNameWithoutFix(path, curLogicNeed);
            if(nextFileName.equals(filename)) break; //没有下一个数据文件
            filename = nextFileName;
        }

        return res;
    }

    private MessageListVO getMessageListVO(Long logicOffset, long offsetLimit, int count, Path path, String filename) throws IOException {
        Path indexPath = Paths.get(path.toAbsolutePath().toString(), filename +".idx");
        Path dataPath = Paths.get(path.toAbsolutePath().toString(), filename +".dat");

        MessageListVO res = null;
        long startOffset = Long.parseLong(filename);
        if(!Files.exists(indexPath)) { //没有索引文件,直接读数据文件

            res = readDataFileAfterOffset(dataPath.toFile(), 0, logicOffset - startOffset, startOffset, offsetLimit, count);
        } else { //有索引

            //读取索引文件
            List<IndexNode> indexList = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(indexPath.toFile()))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    indexList.add(IndexNode.toIndexNode(line));
                }
            }
            indexList.sort((i1,i2)->i1.getLogicOffset().compareTo(i2.getLogicOffset()));
            //二分找在index的位置
            if (indexList.size() == 0) throw new IllegalArgumentException("index file context is empty");
            int i = Collections.binarySearch(indexList, new IndexNode(logicOffset, 0));
            if (i < 0) {
                i = Math.abs(i) - 1;
            }

            File file = dataPath.toFile();
            if(i == 0) { // 没有比当前小offset的索引
                res = readDataFileAfterOffset(file, 0, logicOffset - startOffset, startOffset, offsetLimit, count);
            }else {
                i = Math.min(i, indexList.size()-1);
                IndexNode indexNode = indexList.get(i);
                if(indexNode.getLogicOffset() > logicOffset) {
                    indexNode = indexList.get(Math.max(0, i-1));
                }
                long fileOffset = indexNode.getFileOffset();
                long targetOffset = logicOffset - startOffset;


                res = readDataFileAfterOffset(file, fileOffset, targetOffset, startOffset, offsetLimit, count);
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
                                                  long fileLogicStart,
                                                  long offsetLimit,
                                                  int count) throws IOException {

        System.out.printf("fileOffset %d, targetOffset %d, fileLogicOffset %d\n", fileOffset, targetOffset, fileLogicStart);
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

        long size = Math.min(file.length() - fileOffset, TopicConfig.SINGLE_MESSAGE_LENGTH*count);
        MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, fileOffset, size);

        byte[] lengthBytes = new byte[8];


        List<SaveMessage> messages = new ArrayList<>();

        boolean readable = false;

        long next = 0;
        while (map.position() < map.capacity()) {
            if(map.remaining() < 8){
                break;
            }

            if(!readable && map.position()+fileOffset >= targetOffset) {
                readable = true;
                next = fileLogicStart + map.position()+fileOffset;
            }

            map.get(lengthBytes);

            int length = ByteBuffer.wrap(lengthBytes).getInt();
            //读到文件末尾
            if (Arrays.equals(lengthBytes, BrokerConfig.PERSISTENT_FILE_END_MAGIC)) {
                break;
            }
            if (next >= offsetLimit) break;

            byte[] msgBytes = new byte[(int) length];
            map.get(msgBytes);

            if(readable) {
                next = next + msgBytes.length + 8;
                SaveMessage e = BrokerUtil.brokerSaveBytesTurnMessage(msgBytes);
                e.setLength(length + 8);
                messages.add(e);
                if(messages.size() >= count) break;
            }
        }

        MessageListVO vo = new MessageListVO();
        vo.setMessages(messages);
        vo.setNextOffset(next);
        return vo;
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
