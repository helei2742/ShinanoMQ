package cn.com.shinano.ShinanoMQ.core.datalog;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.dto.IndexNode;
;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.PriorityQueue;

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
        if(isAddIndex(BrokerConfig.PERSISTENT_INDEX_LEVEL)) {
            indexQueue.add(new IndexNode(writePosition, filePosition));
        }
    }


    public void save(String newIndexFileName) throws IOException {

        flush();

        this.indexFileName = newIndexFileName;
    }

    /**
     * 持久化索引文件
     */
    public void flush() throws IOException {
        Path path = Paths.get(this.dir + File.separator + this.indexFileName + ".idx");

        if(!Files.exists(path))
            Files.createFile(path);
        StringBuilder sb = new StringBuilder();

        while (!indexQueue.isEmpty()) {
            IndexNode node = indexQueue.poll();
            // 准备写入的数据
            sb.append(node.toSaveString());
        }

        if(sb.length() == 0) return;

        try(BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile(), true))) {
            bw.write(sb.toString());
            bw.flush();
        }
    }

    private static boolean isAddIndex(int n) {
        double probability = 1.0 / Math.pow(2, n - 1);
        return Math.random() < probability;
    }
}
