package tech.ydb.jdbc.exception;

import tech.ydb.core.StatusCode;

public class YdbNonRetryableException extends YdbExecutionStatusException {
    private static final long serialVersionUID = 1170815831963616837L;

    public YdbNonRetryableException(String message, StatusCode statusCode) {
        super(message, statusCode);
    }
}
