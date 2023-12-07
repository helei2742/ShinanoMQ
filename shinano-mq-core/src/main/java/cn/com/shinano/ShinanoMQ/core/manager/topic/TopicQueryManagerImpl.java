package cn.com.shinano.ShinanoMQ.core.manager.topic;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import cn.com.shinano.ShinanoMQ.base.VO.MessageListVO;
import cn.com.shinano.ShinanoMQ.base.constans.TopicQueryConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.dto.SaveMessage;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.dto.MessageHeader;
import cn.com.shinano.ShinanoMQ.core.store.IndexNode;
import cn.com.shinano.ShinanoMQ.core.manager.AbstractBrokerManager;
import cn.com.shinano.ShinanoMQ.core.manager.OffsetManager;
import cn.com.shinano.ShinanoMQ.core.manager.TopicQueryManager;
import cn.com.shinano.ShinanoMQ.core.store.MappedFile;
import cn.com.shinano.ShinanoMQ.core.store.MappedFileIndex;
import cn.com.shinano.ShinanoMQ.core.store.MappedFileManager;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.file.*;
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

    @Autowired
    private MappedFileManager mappedFileManager;

    /**
     * 查询topic下queue中消息当前的offset
     *
     * @param topic topic
     * @param queue queue
     * @return CompletableFuture<RemotingCommand>
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
     *
     * @param topic topic
     * @param queue queue
     * @param offset offset
     * @param count count
     * @return CompletableFuture<RemotingCommand>
     */
    @Override
    public CompletableFuture<RemotingCommand> queryTopicQueueOffsetMsg(String topic, String queue, long offset, int count) {

        return CompletableFuture.supplyAsync(() -> {
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
     * 查询topic-queue offset 后面的消息，查询的结果写入channel
     * @param topic topic
     * @param queue queue
     * @param offset offset
     * @param transactionId transactionId
     * @param channel channel
     * @return CompletableFuture<RemotingCommand>
     */
    @Override
    public CompletableFuture<RemotingCommand> queryTopicQueueBytesAfterOffset(String topic, String queue, Long offset, String transactionId, Channel channel) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                File dataFile = StoreFileUtil.getDataFileOfLogicOffset(topic, queue, offset);

                long currentOffset = offsetManager.queryTopicQueueOffset(topic, queue);
                if (currentOffset < offset) return RemotingCommand.PARAMS_ERROR;

                int fileOffset = (int) (offset % BrokerConfig.PERSISTENT_FILE_SIZE);


                long readEnd = (offset / BrokerConfig.PERSISTENT_FILE_SIZE == currentOffset / BrokerConfig.PERSISTENT_FILE_SIZE)
                        ? currentOffset % BrokerConfig.PERSISTENT_FILE_SIZE : BrokerConfig.PERSISTENT_FILE_SIZE;
//                RandomAccessFile accessFile = new RandomAccessFile(dataFile, "r");
//                accessFile.seek(fileOffset);
                MappedFile mappedFile = mappedFileManager.getMappedFile(topic, queue, offset);

                MappedByteBuffer buffer = mappedFile.getReadByteBuffer(fileOffset);
                byte[] bytes = new byte[Math.min((int) (readEnd - fileOffset), BrokerConfig.SYNC_PULL_MAX_LENGTH)];
                buffer.get(bytes);
//                byte[] bytes = mappedFile.readBytes(fileOffset, Math.min((int) (readEnd - fileOffset), BrokerConfig.SYNC_PULL_MAX_LENGTH));

//                int read = accessFile.read(bytes);

                RemotingCommand response = new RemotingCommand();
                response.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_PULL_MESSAGE_RESPONSE);
                response.setTransactionId(transactionId);
                response.addExtField(ExtFieldsConstants.SAVE_FILE_NAME, dataFile.getName());
                response.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));
                response.addExtField(ExtFieldsConstants.BODY_LENGTH, String.valueOf(bytes.length));
                response.setCode(RemotingCommandCodeConstants.SUCCESS);
                response.setBody(bytes);

                channel.writeAndFlush(response);
                return response;
            } catch (Exception e) {
                log.error("sync [{}][{}][{}] message from master error", topic, queue, offset, e);
            }
            return null;
        }, executor);
    }


    /**
     * 查询topic queue 中 在offset之后的消息
     *
     * @param topic       topic name
     * @param queue       queue name
     * @param logicOffset logicOffset
     * @param count       从logicOffset开始查几条
     * @return MessageListVO
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


//        Path path = Paths.get(StoreFileUtil.getTopicQueueSaveDir(topic, queue));
//        if (!Files.exists(path)) return MessageListVO.empty();

        //获取数据文件里offset的插入点文件名
//        String filename = StoreFileUtil.getIndexFileNameWithoutFix(path, logicOffset);
        MappedFile mappedFile = mappedFileManager.getMappedFile(topic, queue, logicOffset);

        //跨文件获取
        MessageListVO res = null;
        MessageListVO temp = null;
        int curNeed = count;
        long curLogicNeed = logicOffset;
        while (curNeed != 0) {
            temp = getMessageListVO(curLogicNeed, offsetLimit, curNeed, mappedFile);

            if (res == null) res = temp;
            else {
                res.getMessages().addAll(temp.getMessages());
                res.setNextOffset(temp.getNextOffset());
            }

            curNeed -= temp.getMessages().size();
            curLogicNeed = temp.getNextOffset();

            if (curNeed == 0) break; //获取完

            mappedFile = mappedFileManager.getMappedFile(topic, queue, curLogicNeed);
        }

        return res;
    }

    private MessageListVO getMessageListVO(Long logicOffset, long offsetLimit, int count, MappedFile mappedFile) throws IOException {
        MessageListVO res = null;
        long startOffset = mappedFile.getStartOffset();

        MappedFileIndex index = mappedFile.getIndex();

        long targetOffset = logicOffset - startOffset;

        if (index.length() == 0) { //没有索引文件,直接读数据文件

            res = readDataFileAfterOffset(mappedFile, 0, targetOffset, startOffset, offsetLimit, count);
        } else { //有索引

            IndexNode node = index.searchOffsetPreIndexNode(new IndexNode(logicOffset, 0));
            if(node == null) {
                log.warn("index of mapped file [{}] context is empty", mappedFile.getFilename());
                res = readDataFileAfterOffset(mappedFile, 0, targetOffset, startOffset, offsetLimit, count);
                return res;
            }

            res = readDataFileAfterOffset(mappedFile, node.getFileOffset(), targetOffset, startOffset, offsetLimit, count);

        }
        return res;
    }


    /**
     * 读取数据文件，从fileOffset开始向后找，找到targetOffset处后的放到结果集中
     *
     * @param mappedFile         文件
     * @param fileOffset   读取文件的起始物理偏移
     * @param targetOffset 读取文件目标物理偏移
     * @return 数据文件的消息列表
     * @throws IOException IOException
     */
    private MessageListVO readDataFileAfterOffset(MappedFile mappedFile,
                                                  long fileOffset,
                                                  long targetOffset,
                                                  long fileLogicStart,
                                                  long offsetLimit,
                                                  int count) throws IOException {

        System.out.printf("fileOffset %d, targetOffset %d, fileLogicOffset %d\n", fileOffset, targetOffset, fileLogicStart);
//        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
//        long size = Math.min(file.length() - fileOffset, (long) TopicConfig.SINGLE_MESSAGE_LENGTH * count);
//        MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, fileOffset, size);

        MappedByteBuffer map = mappedFile.getReadByteBuffer((int)fileOffset);

        int headerLength = BrokerConfig.MESSAGE_HEADER_LENGTH;

        byte[] headerBytes = new byte[headerLength];
        byte[] endMagic = new byte[BrokerConfig.PERSISTENT_FILE_END_MAGIC.length];

        List<SaveMessage> messages = new ArrayList<>();

        boolean readable = false;

        long next = 0;
        while (map.position() < map.capacity()) {
            if (map.remaining() < headerLength) { //文件末尾
                next = (((next / BrokerConfig.PERSISTENT_FILE_SIZE) + 1) * BrokerConfig.PERSISTENT_FILE_SIZE);
                break;
            }

            if (!readable && map.position() + fileOffset >= targetOffset) {
                readable = true;
                next = fileLogicStart + map.position() + fileOffset;
            }

            map.get(headerBytes);

            MessageHeader messageHeader = MessageHeader.generateMessageHeader(headerBytes);

            int length = messageHeader.getLength();

            //文件头魔数不对
            if (!Arrays.equals(BrokerConfig.MESSAGE_HEADER_MAGIC, messageHeader.getMagic())) {
                System.arraycopy(headerBytes, 0, endMagic, 0, endMagic.length);
                if (Arrays.equals(endMagic, BrokerConfig.PERSISTENT_FILE_END_MAGIC)) {//读到文件末尾
                    next = (((next / BrokerConfig.PERSISTENT_FILE_SIZE) + 1) * BrokerConfig.PERSISTENT_FILE_SIZE);
                }
                break;
            }
            if (next >= offsetLimit) break;

            byte[] msgBytes = new byte[(int) length];
            map.get(msgBytes);

            if (readable) {
                next = next + msgBytes.length + headerLength;
                SaveMessage e = BrokerUtil.brokerSaveBytesTurnMessage(msgBytes);
                e.setLength(length + headerLength);
                messages.add(e);
                if (messages.size() >= count) break;
            }
        }

        MessageListVO vo = new MessageListVO();
        vo.setMessages(messages);
        vo.setNextOffset(next);
        return vo;
    }


    public static void main(String[] args) {
//        List<Integer> list = new ArrayList<>();
//        list.add(2);
//        list.add(3);
//        list.add(4);
//        list.add(6);
//        list.add(8);
//        list.add(10);
//        System.out.println(Collections.binarySearch(list, 1));
//        System.out.println(Collections.binarySearch(list, 5));
//        System.out.println(Collections.binarySearch(list, 7));
//        System.out.println(Collections.binarySearch(list, 8));
//        System.out.println(Collections.binarySearch(list, 11));


        File file = new File("D:\\develop\\git\\data\\ShinanoMQ\\broker1\\datalog\\test-create1\\queue1\\00000000000000000000.idx");
        System.out.println(file.getName().split("\\.")[0]);
    }
}
