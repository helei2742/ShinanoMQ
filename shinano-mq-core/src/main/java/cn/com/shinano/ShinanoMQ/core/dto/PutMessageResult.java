package cn.com.shinano.ShinanoMQ.core.dto;
import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.dto.RemotingCommand;
import cn.com.shinano.ShinanoMQ.base.pool.RemotingCommandPool;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PutMessageResult {
    private String transactionId;
    private long offset;
    private byte[] content;
    private PutMessageStatus status;

    public PutMessageResult(String tsId, PutMessageStatus status) {
        this.transactionId = tsId;
        this.status = status;
    }

    public PutMessageResult setStatus(PutMessageStatus status) {
        this.status = status;
        return this;
    }

    public RemotingCommand handlePutMessageResult(boolean isSyncMsgToCluster) {
        String tsId = this.getTransactionId();
        RemotingCommand response = RemotingCommandPool.getObject();
        if(isSyncMsgToCluster) {
            response.setFlag(RemotingCommandFlagConstants.PRODUCER_MESSAGE_RESULT);
        }else {
            response.setFlag(RemotingCommandFlagConstants.BROKER_SYNC_SAVE_MESSAGE_RESPONSE);
        }
        response.setTransactionId(tsId);

        switch (this.getStatus()) {
            case REMOTE_SAVE_SUCCESS:
                response.setCode(RemotingCommandCodeConstants.SUCCESS);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.PUT_OK.name());
                break;
            case APPEND_LOCAL:
            case REMOTE_SAVE_FAIL:
                response.addExtField(ExtFieldsConstants.OFFSET_KEY, String.valueOf(offset));
                if(isSyncMsgToCluster) {
                    response.setCode(RemotingCommandCodeConstants.FAIL);
                    response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.REMOTE_SAVE_FAIL.name());
                } else {
                    response.setCode(RemotingCommandCodeConstants.SUCCESS);
                    response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.PUT_OK.name());
                }
                break;
            case FLUSH_DISK_TIMEOUT:
            case CREATE_MAPPED_FILE_FAILED:
            case PROPERTIES_SIZE_EXCEEDED:
            case UNKNOWN_ERROR:
                response.setCode(RemotingCommandCodeConstants.FAIL);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, this.getStatus().name());
                break;
            default:
                response.setCode(RemotingCommandCodeConstants.FAIL);
                response.addExtField(ExtFieldsConstants.PRODUCER_PUT_MESSAGE_RESULT_KEY, PutMessageStatus.UNKNOWN_ERROR.name());
        }
        return response;
    }
}
