package cn.com.shinano.ShinanoMQ.core.datalog;

import cn.com.shinano.ShinanoMQ.core.config.SystemConfig;
import cn.com.shinano.ShinanoMQ.core.dto.IndexNode;
;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.PriorityQueue;
import java.util.concurrent.Future;

public class MappedFileIndex {
    private final PriorityQueue<IndexNode> indexQueue;

    private String indexFileName;

    private final String dir;

    public MappedFileIndex(String dir, String indexFileName) {
        this.indexFileName = indexFileName;
        this.dir = dir;
        this.indexQueue = new PriorityQueue<>((i1, i2)->i1.getLogicOffset().compareTo(i2.getLogicOffset()));
    }


    /**
     * 添加索引
     * @param writePosition 逻辑上写的位置
     * @param filePosition  物理文件上写的位置
     */
    public void updateIndex(Long writePosition, Long filePosition) {
        if(isAddIndex(SystemConfig.PERSISTENT_INDEX_LEVEL)) {
            indexQueue.add(new IndexNode(writePosition, filePosition));
        }
    }

    /**
     * 持久化索引文件
     */
    public void save(String newIndexFileName) throws IOException {
        Path path = Paths.get(this.dir + File.separator + this.indexFileName + ".idx");

        if(!Files.exists(path))
            Files.createFile(path);

        AsynchronousFileChannel fileChannel =
                null;
        fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);

        long position = 0;
        while (!indexQueue.isEmpty()) {
            IndexNode node = indexQueue.poll();
            // 准备写入的数据
            byte[] bytes = node.toSaveBytes();
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            // 异步写入文件
            Future<Integer> result = fileChannel.write(buffer, position);
            position += bytes.length;
        }

        this.indexFileName = newIndexFileName;
    }

    private static boolean isAddIndex(int n) {
        double probability = 1.0 / Math.pow(2, n - 1);
        return Math.random() < probability;
    }
}
