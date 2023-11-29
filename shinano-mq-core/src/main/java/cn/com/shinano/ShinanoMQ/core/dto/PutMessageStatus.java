package cn.com.shinano.ShinanoMQ.core.dto;

public enum PutMessageStatus {
        PUT_OK,
        APPEND_LOCAL,
        FLUSH_DISK_TIMEOUT,
        CREATE_MAPPED_FILE_FAILED,
        PROPERTIES_SIZE_EXCEEDED,
        UNKNOWN_ERROR,
        REMOTE_SAVE_SUCCESS,
        REMOTE_SAVE_FAIL
}