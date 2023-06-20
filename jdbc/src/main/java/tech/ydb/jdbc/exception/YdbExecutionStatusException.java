package tech.ydb.jdbc.exception;

import tech.ydb.core.StatusCode;

public class YdbExecutionStatusException extends YdbExecutionException {
    private static final long serialVersionUID = 4476269562160877309L;

    private final StatusCode statusCode;

    public YdbExecutionStatusException(String message, StatusCode statusCode) {
        super(message, null, statusCode.getCode());
        this.statusCode = statusCode;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }
}
