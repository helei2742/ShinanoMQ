package cn.com.shinano.ShinanoMQ.core.store;

import cn.com.shinano.ShinanoMQ.core.config.BrokerConfig;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class MappedFileIndex {
    private PriorityQueue<IndexNode> indexQueue;

    private String indexFileName;

    private boolean changed;

    private final Path path;

    public MappedFileIndex(String dir, String indexFileName) {
        this.indexFileName = indexFileName;
        this.indexQueue = new PriorityQueue<>((i1, i2) -> i1.getLogicOffset().compareTo(i2.getLogicOffset()));

        path = Paths.get(dir + File.separator + indexFileName + ".idx");
        if (Files.exists(path)) {
            try {
                List<String> lines = Files.readAllLines(path);
                for (String line : lines) {
                    indexQueue.offer(IndexNode.toIndexNode(line));
                }
            } catch (IOException e) {
                throw new RuntimeException("mapped file index read error");
            }
        }
        this.changed = false;
    }


    /**
     * 添加索引
     *
     * @param writePosition 逻辑上写的位置
     * @param filePosition  物理文件上写的位置
     */
    public void updateIndex(long writePosition, int filePosition) {
        if (isAddIndex(BrokerConfig.PERSISTENT_INDEX_LEVEL)) {
            indexQueue.add(new IndexNode(writePosition, filePosition));
            this.changed = true;
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
        if (indexQueue.isEmpty() || !changed) return;
        changed = false;

        if (!Files.exists(path))
            Files.createFile(path);

        StringBuilder sb = new StringBuilder();

        PriorityQueue<IndexNode> pq = new PriorityQueue<>((i1, i2) -> i1.getLogicOffset().compareTo(i2.getLogicOffset()));
        while (!indexQueue.isEmpty()) {
            IndexNode node = indexQueue.poll();
            // 准备写入的数据
            sb.append(node.toSaveString());
            pq.offer(node);
        }

        indexQueue = pq;

        if (sb.length() == 0) return;

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile(), true))) {
            bw.write(sb.toString());
            bw.flush();
        }
    }

    private static boolean isAddIndex(int n) {
        double probability = 1.0 / Math.pow(2, n - 1);
        return Math.random() < probability;
    }

    public int length() {
        return indexQueue.size();
    }

    public IndexNode searchOffsetPreIndexNode(IndexNode searchNode) {
        List<IndexNode> indexList = new ArrayList<>(indexQueue);

        indexList.sort((i1, i2) -> i1.getLogicOffset().compareTo(i2.getLogicOffset()));
        //二分找在index的位置
        if (indexList.size() == 0) {
            return null;
        }
        int i = Collections.binarySearch(indexList, searchNode);
        if (i < 0) {
            i = Math.abs(i) - 1;
        }

        if (i == 0) { // 没有比当前小offset的索引
            return null;
        } else {
            i = Math.min(i, indexList.size() - 1);
            IndexNode indexNode = indexList.get(i);
            if (indexNode.getLogicOffset() > searchNode.getLogicOffset()) {
                indexNode = indexList.get(Math.max(0, i - 1));
            }
            return indexNode;
        }
    }
}
