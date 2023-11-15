package cn.com.shinano.ShinanoMQ.core.dto;

public enum PutMessageStatus {
        PUT_OK,
        FLUSH_DISK_TIMEOUT,
        SERVICE_NOT_AVAILABLE,
        CREATE_MAPPED_FILE_FAILED,
        PROPERTIES_SIZE_EXCEEDED,
        UNKNOWN_ERROR,
        PERSISTENT_MESSAGE_FAIL
}