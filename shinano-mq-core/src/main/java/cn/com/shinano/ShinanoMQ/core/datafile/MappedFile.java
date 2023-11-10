package cn.com.shinano.ShinanoMQ.core.datafile;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class MappedFile {

    private static final ConcurrentMap<String,MappedFile> existMappedFileMap = new ConcurrentHashMap<>();

    protected static final AtomicLongFieldUpdater<MappedFile> WRITE_POSITION_UPDATER;
    protected static final AtomicLongFieldUpdater<MappedFile> FILE_POSITION_UPDATER;


    private File file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private volatile long writePosition; //逻辑上写的位置
    private volatile long filePosition; //物理上写的位置

    private final Long fileSize;
    private final String fileDir;

    private long lastFlushTime = -1L;
    /**
     * 当前MappedFile文件的索引
     */
    private MappedFileIndex index;

    static {
        WRITE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(MappedFile.class, "writePosition");
        FILE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(MappedFile.class, "filePosition");
    }

    /**
     * 创建MappedFile对象
     * @param writePosition 映射文件逻辑写的位置
     * @param filePosition 文件实际写的位置
     * @param fileLimit 文件允许写的大小
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

        this.fileSize = BrokerConfig.PERSISTENT_FILE_SIZE;
        WRITE_POSITION_UPDATER.set(this, writePosition);
        FILE_POSITION_UPDATER.set(this, filePosition);

        this.index = new MappedFileIndex(fileDir, this.file.getName().replace(".dat",""));

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
        long currentPos = WRITE_POSITION_UPDATER.get(this);
        long filePos = FILE_POSITION_UPDATER.get(this);

        if(currentPos + bytes.length > mappedByteBuffer.capacity()) {
            //装不下了，重新map一块装
            //TODO file快装满的时候提前
            synchronized (this) {
                currentPos = WRITE_POSITION_UPDATER.get(this);

                if(currentPos + bytes.length > mappedByteBuffer.capacity()) {
                    //原来的先刷盘
                    mappedByteBuffer.force();

                    //保存索引文件
                    index.save(BrokerUtil.getSaveFileName(writePosition));

                    //新搞一个
                    this.file = newFile(writePosition);
                    this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();

                    FILE_POSITION_UPDATER.set(this, 0);
                    this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
                }
            }
        }

        this.mappedByteBuffer.put(bytes);

        currentPos = WRITE_POSITION_UPDATER.addAndGet(this, bytes.length);
        filePos = FILE_POSITION_UPDATER.addAndGet(this, bytes.length);
        System.out.println(bytes.length + "---" + currentPos + "---" + filePos);
        //更新内存中的索引
        index.updateIndex(currentPos, filePos);

        return currentPos;
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
    public static MappedFile getMappedFile(String topic, String queue, long logicOffset) throws IOException {
        String mappedFileKey = BrokerUtil.makeTopicQueueKey(topic, queue);

        if(!existMappedFileMap.containsKey(mappedFileKey)) {
            synchronized (mappedFileKey.intern()) {
                if(!existMappedFileMap.containsKey(mappedFileKey)) {

                    File dirFile = new File(BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue);
                    if (!dirFile.exists()) dirFile.mkdirs();

                    File[] dataLogs = dirFile.listFiles();

                    MappedFile res = null;
                    if (dataLogs == null || dataLogs.length == 0) {//新的topic-queue
                        res = new MappedFile(0, 0, BrokerConfig.PERSISTENT_FILE_SIZE, dirFile);
                    } else { //里面有历史消息
                        File newest = BrokerUtil.getNewestPersistentFile(dataLogs);

                        long fileLogicStart = Long.parseLong(newest.getName().split("\\.")[0]);

                        long fileUsedLength = logicOffset - fileLogicStart;

                        if (fileUsedLength >= BrokerConfig.PERSISTENT_FILE_SIZE) { //文件已经写满
                            res = new MappedFile(logicOffset, 0, BrokerConfig.PERSISTENT_FILE_SIZE, dirFile);
                        } else {
                            res = new MappedFile(logicOffset, fileUsedLength,
                                    BrokerConfig.PERSISTENT_FILE_SIZE-fileUsedLength, newest);
                        }
                    }
                    existMappedFileMap.put(mappedFileKey, res);
                }
            }
        }
        return existMappedFileMap.get(mappedFileKey);
    }

    public void flush() throws IOException {
        //原来的先刷盘
        mappedByteBuffer.force();

        //保存索引文件
        index.flush();

        this.lastFlushTime = System.currentTimeMillis();
    }


    public long getLastFlushTime() {
        return this.lastFlushTime;
    }
}
