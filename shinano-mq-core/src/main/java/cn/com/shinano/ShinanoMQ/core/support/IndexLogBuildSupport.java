package cn.com.shinano.ShinanoMQ.core.support;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
@Slf4j
@Component
public class IndexLogBuildSupport {

//    private static final ExecutorService executor = Executors.newFixedThreadPool(2);

    /**
     *  以行的格式存储， <offset, count>
     * @param topic
     * @param queue
     * @param wroteOffset
     */

    public void buildIndexLog(String topic, String queue, long wroteOffset) {

    }
}
