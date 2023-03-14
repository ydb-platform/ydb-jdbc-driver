package tech.ydb.jdbc.exception;

import tech.ydb.core.StatusCode;

public class YdbRetryableException extends YdbExecutionStatusException {
    private static final long serialVersionUID = 688604408491567864L;

    public YdbRetryableException(String message, StatusCode statusCode) {
        super(message, statusCode);
    }
}
