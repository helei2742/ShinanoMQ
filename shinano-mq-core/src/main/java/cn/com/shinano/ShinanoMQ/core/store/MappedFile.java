package cn.com.shinano.ShinanoMQ.core.store;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.config.TopicConfig;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MappedFile {

    private static final ConcurrentMap<String, MappedFile> existMappedFileMap = new ConcurrentHashMap<>();

    protected static final AtomicLongFieldUpdater<MappedFile> WRITE_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<MappedFile> FILE_POSITION_UPDATER;


    private File file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private volatile long writePosition; //逻辑上写的位置
    private volatile int filePosition; //物理上写的位置

    private long startOffset;

    private final Long fileSize;
    private final String fileDir;

    private long lastFlushTime = -1L;

    private AtomicBoolean appendable;

    private final int HERDER_OFFSET_POSITION = BrokerConfig.MESSAGE_HEADER_LENGTH_LENGTH + BrokerConfig.MESSAGE_HEADER_MAGIC.length;

    /**
     * 当前MappedFile文件的索引
     */
    private final MappedFileIndex index;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    static {
        WRITE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(MappedFile.class, "writePosition");
        FILE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MappedFile.class, "filePosition");
    }

    /**
     * 创建MappedFile对象
     *
     * @param writePosition 映射文件逻辑写的位置
     * @param filePosition  文件实际写的位置
     * @param fileLimit     文件允许写的大小
     * @param file          传入文件时，在该文件里写；传入文件夹时新建writePosition名字的文件
     * @throws IOException
     */
    public MappedFile(long writePosition,
                      int filePosition,
                      long fileLimit,
                      File file) throws IOException {
        if (file.isDirectory()) {
            this.fileDir = file.getAbsolutePath();
            this.file = newFile(writePosition);
        } else {
            this.file = file;
            this.fileDir = file.getParentFile().getAbsolutePath();
        }

        this.fileSize = BrokerConfig.PERSISTENT_FILE_SIZE;
        this.startOffset = (writePosition/fileSize) * fileSize;
        WRITE_POSITION_UPDATER.set(this, writePosition);
        FILE_POSITION_UPDATER.set(this, filePosition);

        this.index = new MappedFileIndex(fileDir, this.file.getName().replace(".dat", ""));
        this.appendable = new AtomicBoolean(true);

        init(filePosition, fileLimit);
    }

    private void init(long filePosition, long limit) throws IOException {
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, limit);
        this.mappedByteBuffer.load();
        this.mappedByteBuffer.position((int) filePosition);
        System.out.println("----" + mappedByteBuffer);
    }

    public AtomicLong counter = new AtomicLong();


    /**
     * 向文件中追加内容
     *
     * @param message
     * @return
     * @throws IOException
     */
    @Deprecated
    public AppendMessageResult append(Message message) {
        int filePos = FILE_POSITION_UPDATER.get(this);
        long writePos = WRITE_POSITION_UPDATER.get(this);

        byte[] bytes = BrokerUtil.messageTurnBrokerSaveBytes(message, writePos);

        if (bytes.length > TopicConfig.SINGLE_MESSAGE_LENGTH) {
            return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, writePos,
                    bytes.length, null, message.getTransactionId(), System.currentTimeMillis());
        }

        if (filePos + bytes.length >
                mappedByteBuffer.capacity() - BrokerConfig.PERSISTENT_FILE_END_MAGIC.length) {
            return new AppendMessageResult(AppendMessageStatus.END_OF_FILE,
                    writePos,
                    bytes.length,
                    bytes,
                    message.getTransactionId(),
                    System.currentTimeMillis());
        }

        this.mappedByteBuffer.put(bytes);
        //更新内存中的索引
        index.updateIndex(writePos, filePos);

        writePos = WRITE_POSITION_UPDATER.addAndGet(this, bytes.length);
        filePos = FILE_POSITION_UPDATER.addAndGet(this, bytes.length);

        return new AppendMessageResult(AppendMessageStatus.PUT_OK, writePos, bytes.length,
                bytes, message.getTransactionId(), System.currentTimeMillis());
    }

    public AppendMessageResult append(byte[] bytes, long startOffset, boolean normalAppend) {
        int filePos = FILE_POSITION_UPDATER.get(this);
        long writePos = WRITE_POSITION_UPDATER.get(this);

        if (startOffset != writePos) {
            return new AppendMessageResult(AppendMessageStatus.WRITE_POSITION_ERROR, writePos,
                    bytes.length, null, null, System.currentTimeMillis());
        }

        if (bytes.length > TopicConfig.SINGLE_MESSAGE_LENGTH) {
            return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, writePos,
                    bytes.length, null, null, System.currentTimeMillis());
        }

        if (filePos + bytes.length >
                mappedByteBuffer.capacity() - BrokerConfig.PERSISTENT_FILE_END_MAGIC.length && normalAppend) {

            if (mappedByteBuffer.remaining() >= BrokerConfig.PERSISTENT_FILE_END_MAGIC.length) {
                //文件末尾写入魔数
                mappedByteBuffer.put(BrokerConfig.PERSISTENT_FILE_END_MAGIC);
            }

            this.appendable.set(false);

            long nextOffset = ((writePos / fileSize) + 1) * fileSize;
            return new AppendMessageResult(AppendMessageStatus.END_OF_FILE,
                    nextOffset,
                    bytes.length,
                    bytes,
                    null,
                    System.currentTimeMillis());
        }


        if (normalAppend) { //是普通的的插入单条消息
            //更新内存中的索引
            index.updateIndex(writePos, filePos);

            //更新消息头字段
            System.arraycopy(ByteBuffer.allocate(BrokerConfig.MESSAGE_HEADER_LOG_OFFSET_LENGTH).putLong(filePos).array(),
                    0, bytes, HERDER_OFFSET_POSITION, BrokerConfig.MESSAGE_HEADER_LOG_OFFSET_LENGTH);
            System.arraycopy(ByteBuffer.allocate(BrokerConfig.MESSAGE_HEADER_LOGIC_OFFSET_LENGTH).putLong(writePos).array(),
                    0, bytes, HERDER_OFFSET_POSITION + BrokerConfig.MESSAGE_HEADER_LOG_OFFSET_LENGTH,
                    BrokerConfig.MESSAGE_HEADER_LOGIC_OFFSET_LENGTH);
        }

        this.mappedByteBuffer.put(bytes);

        writePos = WRITE_POSITION_UPDATER.addAndGet(this, bytes.length);
        filePos = FILE_POSITION_UPDATER.addAndGet(this, bytes.length);

        return new AppendMessageResult(AppendMessageStatus.PUT_OK, writePos, bytes.length,
                bytes, null, System.currentTimeMillis());
    }


    /**
     * 新创建一个File对象
     *
     * @param startOffset 文件开始的offset
     * @return
     */
    private File newFile(Long startOffset) {
        String pathname = this.fileDir + File.separator + StoreFileUtil.getSaveFileName(startOffset) + ".dat";
        System.out.println("pathname:[]" + pathname);
        return new File(pathname);
    }


    /**
     * 获取MappedFile对象
     *
     * @param topic topic name
     * @param queue queue name
     * @return
     * @throws IOException
     */
    @Deprecated
    public static MappedFile getMappedFile(String topic, String queue, long logicOffset) throws IOException {
        String mappedFileKey = BrokerUtil.makeTopicQueueKey(topic, queue);

        if (!existMappedFileMap.containsKey(mappedFileKey)) {
            synchronized (getLockStr(mappedFileKey)) {
                if (!existMappedFileMap.containsKey(mappedFileKey)) {

                    File dirFile = new File(BrokerConfig.PERSISTENT_FILE_LOCATION + File.separator + topic + File.separator + queue);
                    if (!dirFile.exists()) dirFile.mkdirs();

                    File[] dataLogs = dirFile.listFiles();

                    MappedFile res = null;
                    if (dataLogs == null || dataLogs.length == 0) {//新的topic-queue
                        res = new MappedFile(0, 0, BrokerConfig.PERSISTENT_FILE_SIZE, dirFile);
                    } else { //里面有历史消息数据
                        File newest = StoreFileUtil.getNewestPersistentFile(dataLogs);

                        long fileLogicStart = Long.parseLong(newest.getName().split("\\.")[0]);

                        int fileUsedLength = (int) (logicOffset - fileLogicStart);

                        if (fileUsedLength >= BrokerConfig.PERSISTENT_FILE_SIZE) { //文件已经写满
                            res = new MappedFile(logicOffset, 0, BrokerConfig.PERSISTENT_FILE_SIZE, dirFile);
                        } else {
                            res = new MappedFile(logicOffset, fileUsedLength,
                                    BrokerConfig.PERSISTENT_FILE_SIZE - fileUsedLength, newest);
                        }
                    }
                    existMappedFileMap.put(mappedFileKey, res);
                }
            }
        }
        return existMappedFileMap.get(mappedFileKey);
    }


    private static String getLockStr(String key) {
        return ("LOCK-" + MappedFile.class.getSimpleName() + "-" + key).intern();
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


    public static void flushMappedFiles() throws IOException {
        for (Map.Entry<String, MappedFile> entry : existMappedFileMap.entrySet()) {
            MappedFile mappedFile = entry.getValue();

            if (mappedFile == null) continue;

            if (System.currentTimeMillis() - mappedFile.getLastFlushTime() >= 10000) {
                mappedFile.flush();
            }
        }
    }


    public void loadNextFile(long writePos) {
        //装不下了，重新map一块装
        try {
            //文件结尾魔数
            mappedByteBuffer.put(BrokerConfig.PERSISTENT_FILE_END_MAGIC);
            mappedByteBuffer.force();

            //保存索引文件
            index.save(StoreFileUtil.getSaveFileName(writePos));

            //新搞一个
            this.file = newFile(writePos);
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            FILE_POSITION_UPDATER.set(this, 0);
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public long getWritePos() {
        return WRITE_POSITION_UPDATER.get(this);
    }

    public boolean appendAble() {
        return this.appendable.get();
    }

    public String getFilename() {
        return file.getName();
    }

    public byte[] readBytes(int offset, int length) {
        lock.readLock().lock();
        try {
            byte[] bytes = new byte[length];
            ByteBuffer slice = mappedByteBuffer.slice();
            slice.position(offset);
            slice.get(bytes);
            return bytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    public MappedByteBuffer getReadByteBuffer(int fileOffset) {
        lock.writeLock().lock();
        try {
            mappedByteBuffer.position(fileOffset);
            ByteBuffer slice = mappedByteBuffer.slice().asReadOnlyBuffer();
            mappedByteBuffer.position(this.filePosition);
            return (MappedByteBuffer) slice;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getStartOffset() {
        return startOffset;
    }

    public MappedFileIndex getIndex() {
        return index;
    }
}
