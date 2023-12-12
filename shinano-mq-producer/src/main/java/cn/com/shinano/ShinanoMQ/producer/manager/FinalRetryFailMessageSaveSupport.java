package cn.com.shinano.ShinanoMQ.producer.manager;

import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author lhe.shinano
 * @date 2023/12/12
 */
@Slf4j
public class FinalRetryFailMessageSaveSupport {

    private static final String FILE_NAME_PATTERN = "%s_final_retry_fail_request.dat";

    private static final String DIR = System.getProperty("user.dir");

    public static void appendFinalRetryFailMessage(RemotingCommand request, String clientId) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(DIR + File.separator + String.format(FILE_NAME_PATTERN, clientId), true))) {
            bw.write(JSON.toJSONString(request));
            bw.newLine();
        } catch (IOException e) {
            log.error("write final retry fail message error, request command [{}], clientId [{}]", request, clientId);
        }
    }
}
