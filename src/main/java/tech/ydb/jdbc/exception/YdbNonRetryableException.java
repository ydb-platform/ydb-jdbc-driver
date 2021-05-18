package tech.ydb.jdbc.exception;

import tech.ydb.core.StatusCode;

public class YdbNonRetryableException extends YdbExecutionStatusException {

    public YdbNonRetryableException(Object response, StatusCode statusCode) {
        super(response, statusCode);
    }
}
