package cn.com.shinano.ShinanoMQ.base.VO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * @author lhe.shinano
 * @date 2023/11/10
 */
@Data
@NoArgsConstructor
public class BatchAckVO {
    private List<String> successTsIdLIst;
    private List<String> failTsIdList;

    public BatchAckVO(Set<String> successTsIdLIst, Set<String> failTsIdList) {
        this.successTsIdLIst = new ArrayList<>(successTsIdLIst);
        this.failTsIdList = new ArrayList<>(failTsIdList);
    }
}
