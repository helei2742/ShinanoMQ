package cn.com.shinano.ShinanoMQ.core.datalog;

import cn.com.shinano.ShinanoMQ.core.config.SystemConfig;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MappedFile {

    private static final ConcurrentMap<String,MappedFile> existMappedFileMap = new ConcurrentHashMap<>();

    private File file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private Long writePosition; //逻辑上写的位置
    private Long filePosition; //物理上写的位置

    private final Long fileSize;
    private final String fileDir;

    /**
     * 当前MappedFile文件的索引
     */
    private MappedFileIndex index;

    /**
     * 创建MappedFile对象
     * @param writePosition 映射文件逻辑写的位置
     * @param filePosition 文件实际写的位置
     * @param fileLimit 文件运行写的大小
     * @param file 传入文件时，在该文件里写；传入文件夹时新建writePosition名字的文件
     * @throws IOException
     */
    protected MappedFile(long writePosition,
                         long filePosition,
                         long fileLimit,
                         File file) throws IOException {
        if(file.isDirectory()) {
            this.fileDir = file.getAbsolutePath();
            this.file = newFile(writePosition);
        }else {
            this.file = file;
            this.fileDir = file.getParentFile().getAbsolutePath();
        }

        this.fileSize = SystemConfig.PERSISTENT_FILE_SIZE;
        this.writePosition = writePosition;
        this.filePosition = filePosition;
        this.index = new MappedFileIndex(fileDir, BrokerUtil.getSaveFileName(writePosition));

        init(filePosition, fileLimit);
    }

    private void init(long filePosition, long limit) throws IOException {
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, filePosition, limit);
        System.out.println("----"+mappedByteBuffer);
    }

    /**
     * 向文件中追加内容
     * @param bytes
     * @return
     * @throws IOException
     */
    public long append(byte[] bytes) throws IOException {
        if(mappedByteBuffer.position() + bytes.length > mappedByteBuffer.capacity()) {
            //装不下了，重新map一块装
            getNewMappedByteBuffer();
        }
        this.mappedByteBuffer.put(bytes);

        this.writePosition += bytes.length;
        this.filePosition += bytes.length;

        //更新内存中的索引
        index.updateIndex(writePosition, filePosition);

        return writePosition;
    }

    /**
     * 当前的写满了，映射到下一个文件
     * @throws IOException
     */
    private void getNewMappedByteBuffer() throws IOException {
        //原来的先刷盘
        mappedByteBuffer.force();

        //保存索引文件
        index.save(BrokerUtil.getSaveFileName(writePosition));

        //新搞一个
        this.file = newFile(writePosition);
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.filePosition = 0L;
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
    }

    /**
     * 新创建一个File对象
     * @param startOffset 文件开始的offset
     * @return
     */
    private File newFile(Long startOffset) {
        String pathname = this.fileDir + File.separator + BrokerUtil.getSaveFileName(startOffset) + ".dat";
        System.out.println("pathname:[]"+pathname);
        return new File(pathname);
    }


    /**
     * 获取MappedFile对象
     * @param topic topic name
     * @param queue queue name
     * @return
     * @throws IOException
     */
    public static MappedFile getMappedFile(String topic, String queue) throws IOException {
        String mappedFileKey = BrokerUtil.makeTopicQueueKey(topic, queue);

        if(!existMappedFileMap.containsKey(mappedFileKey)) {
            synchronized (mappedFileKey.intern()) {
                if(!existMappedFileMap.containsKey(mappedFileKey)) {

                    File dirFile = new File(SystemConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue);
                    if (!dirFile.exists()) dirFile.mkdirs();

                    File[] dataLogs = dirFile.listFiles();

                    MappedFile res = null;
                    if (dataLogs == null || dataLogs.length == 0) {//新的topic-queue
                        res = new MappedFile(0, 0, SystemConfig.PERSISTENT_FILE_SIZE, dirFile);
                    } else { //里面有历史消息
                        File newest = BrokerUtil.getNewestPersistentFile(dataLogs);

                        long fileLogicStart = Long.parseLong(newest.getName().split("\\.")[0]);
                        long fileLength = newest.length();
                        long startOffset = fileLogicStart + fileLength;

                        if (fileLength >= SystemConfig.PERSISTENT_FILE_SIZE) { //文件已经写满
                            res = new MappedFile(startOffset, 0, SystemConfig.PERSISTENT_FILE_SIZE, dirFile);
                        } else {
                            res = new MappedFile(startOffset, fileLength,
                                    SystemConfig.PERSISTENT_FILE_SIZE - fileLength, newest);
                        }
                    }
                    existMappedFileMap.put(mappedFileKey, res);
                }
            }
        }
        return existMappedFileMap.get(mappedFileKey);
    }
}
