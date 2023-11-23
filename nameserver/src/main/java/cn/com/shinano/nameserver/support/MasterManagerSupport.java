package cn.com.shinano.nameserver.support;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.nameserver.NameServerService;

import cn.com.shinano.nameserver.dto.ClusterHost;
import cn.com.shinano.nameserver.dto.NameServerState;
import cn.com.shinano.nameserver.dto.VoteInfo;
import com.alibaba.fastjson.JSON;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;


/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
public class MasterManagerSupport {

    private static final HashSet<VoteInfo> voteSet = new HashSet<>();


    public synchronized static ClusterHost tryVoteMaster(NameServerService nameServerService, RemotingCommand command) {
        ClusterHost master = null;
        switch (nameServerService.getState()) {
            case VOTE:
            case JUST_START:
                ClusterHost selected = JSON.parseObject(command.getExtFieldsValue(ExtFieldsConstants.NAMESERVER_VOTE_MASTER), ClusterHost.class);
                Long startTimeStamp = command.getExtFieldsLong(ExtFieldsConstants.NAMESERVER_START_TIMESTAMP);
                VoteInfo voteInfo = new VoteInfo(startTimeStamp, selected);
                voteSet.add(voteInfo);
                voteSet.add(new VoteInfo(nameServerService.getStartTime(), nameServerService.getServerHost()));

                Optional<VoteInfo> first = voteSet.stream().min(VoteInfo::compareTo);
                if(first.isPresent()){
                    VoteInfo v = first.get();
                    nameServerService.publishNewMaster(v);

                    if(voteSet.size() >= nameServerService.getClusterConnectMap().keySet().size() / 2) {
                        nameServerService.setState(NameServerState.RUNNING);
                    }
                }
                break;
            case RUNNING:
                master = nameServerService.getMaster();
                break;
            default:
                break;
        }
        return master;
    }

    public synchronized static void removeVoteInfo(ClusterHost clusterHost) {
        voteSet.removeIf(voteInfo -> voteInfo.getVoteMaster().equals(clusterHost));
    }


    public static RemotingCommand voteRemoteMessageBuilder(long startTime, ClusterHost clusterHost) {
        RemotingCommand command = new RemotingCommand();
        command.setFlag(RemotingCommandFlagConstants.NAMESERVER_VOTE_MASTER);

        command.addExtField(ExtFieldsConstants.NAMESERVER_VOTE_MASTER, JSON.toJSONString(clusterHost));
        command.addExtField(ExtFieldsConstants.NAMESERVER_START_TIMESTAMP, String.valueOf(startTime));

        return command;
    }

    public static RemotingCommand setMasterRemoteMessageBuilder(ClusterHost master) {
        RemotingCommand command = new RemotingCommand();

        command.setFlag(RemotingCommandFlagConstants.NAMESERVER_SET_MASTER);
        command.addExtField(ExtFieldsConstants.NAMESERVER_VOTE_MASTER, JSON.toJSONString(master));
        return command;
    }
}
