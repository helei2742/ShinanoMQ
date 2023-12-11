package cn.com.shinano.ShinanoMQ.base.dto;

import cn.com.shinano.ShinanoMQ.base.constans.ExtFieldsConstants;
import cn.com.shinano.ShinanoMQ.base.constans.LanguageCode;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandCodeConstants;
import cn.com.shinano.ShinanoMQ.base.constans.RemotingCommandFlagConstants;
import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;

/**
 * @author lhe.shinano
 * @date 2023/11/16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RemotingCommand {

    public final static RemotingCommand TIME_OUT_COMMAND;
    public static final RemotingCommand PARAMS_ERROR;

    static {
        TIME_OUT_COMMAND = new RemotingCommand();
        TIME_OUT_COMMAND.setFlag(RemotingCommandFlagConstants.TIME_OUT_EXCEPTION);
        TIME_OUT_COMMAND.setCode(RemotingCommandCodeConstants.FAIL);

        PARAMS_ERROR = new RemotingCommand();
        PARAMS_ERROR.setCode(RemotingCommandCodeConstants.PARAMS_ERROR);
    }

    private Integer flag;
    private Integer code;
    private LanguageCode language = LanguageCode.JAVA;
    private Integer version = 0;
    private String remark;
    private HashMap<String, String> extFields;

    private byte[] body;

    private Object payLoad;

    @Override
    public RemotingCommand clone()  {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(this.flag);
        remotingCommand.setCode(this.code);
        remotingCommand.setLanguage(this.language);
        remotingCommand.setVersion(this.version);
        remotingCommand.setRemark(this.remark);
        remotingCommand.setExtFields(this.extFields);
        remotingCommand.setBody(this.body);
        remotingCommand.setPayLoad(this.payLoad);
        return remotingCommand;
    }

    public String getTopic() {
        return getExtFieldsValue(ExtFieldsConstants.TOPIC_KEY);
    }

    public String getQueue() {
        return getExtFieldsValue(ExtFieldsConstants.QUEUE_KEY);
    }

    public String getTransactionId() {
        return getExtFieldsValue(ExtFieldsConstants.TRANSACTION_ID_KEY);
    }

    public void setTransactionId(String tsId) {
        addExtField(ExtFieldsConstants.TRANSACTION_ID_KEY, tsId);
    }


    public String getClientId() {
        return getExtFieldsValue(ExtFieldsConstants.CLIENT_ID_KEY);
    }

    public void setClientId(String clientId) {
        addExtField(ExtFieldsConstants.CLIENT_ID_KEY, clientId);
    }

    public String getExtFieldsValue(String extFieldsKey) {
        if(extFields == null) return null;
        return extFields.get(extFieldsKey);
    }

    public Integer getExtFieldsInt(String extFieldsKey) {
        String value = getExtFieldsValue(extFieldsKey);
        if(value == null || "".equals(value)) return null;
        return Integer.parseInt(value);
    }

    public Long getExtFieldsLong(String extFieldsKey) {
        String value = getExtFieldsValue(extFieldsKey);
        if(value == null || "".equals(value)) return null;
        return Long.parseLong(value);
    }

    public void addExtField(String key, String value) {
        if(this.extFields == null) {
            this.extFields = new HashMap<>();
        }
        this.extFields.put(key, value);
    }


    public void release(){}

    protected void clear() {
        this.flag = null;
        this.code = null;
        this.language = null;
        this.version = null;
        this.remark = null;
        if(extFields == null) extFields = new HashMap<>();
        else this.extFields.clear();
        this.body = null;
    }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "flag=" + flag +
                ", code=" + code +
                ", language=" + language +
                ", version=" + version +
                ", remark='" + remark + '\'' +
                ", extFields=" + extFields +
                ", body=" + ((body==null||body.length==0)?"empty":"not empty") +
                '}';
    }
}
