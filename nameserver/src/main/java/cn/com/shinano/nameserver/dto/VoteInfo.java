package cn.com.shinano.nameserver.dto;

import cn.com.shinano.ShinanoMQ.base.dto.ClusterHost;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class VoteInfo implements Comparable<VoteInfo>{
    private long startTimeStamp;
    private ClusterHost voteMaster;

    @Override
    public int compareTo(VoteInfo o) {
        int compare = Long.compare(startTimeStamp, o.startTimeStamp);
        if(compare != 0) return compare;

        compare = voteMaster.compareTo(o.voteMaster);
        if(compare != 0) return compare;
        else throw new IllegalArgumentException("client "+ voteMaster +" can not equal");
    }
}
