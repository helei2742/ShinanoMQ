package cn.com.shinano.nameserver.dto;

/**
 * @author lhe.shinano
 * @date 2023/11/24
 */
public enum RegistryState {
    OK,
    PARAM_ERROR,
    VALIDATE_ACCESS,
    APPEND_LOCAL,
    BROADCAST_SLAVE,
    BROADCAST_SLAVE_FAIL,
    FORWARD_MASTER,
    FORWARD_MASTER_FAIL,
    UNKNOW_ERROR
    ;
}
