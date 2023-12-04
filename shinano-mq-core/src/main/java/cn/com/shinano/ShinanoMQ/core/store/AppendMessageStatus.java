package cn.com.shinano.ShinanoMQ.core.store;

/**
 * When write a message to the commit log, returns code
 */
public enum AppendMessageStatus {
    PUT_OK,
    END_OF_FILE,
    MESSAGE_SIZE_EXCEEDED,
    PROPERTIES_SIZE_EXCEEDED,
    WRITE_POSITION_ERROR,
    UNKNOWN_ERROR,
}
