package tech.ydb.jdbc.exception;

import tech.ydb.core.StatusCode;

public class YdbExecutionStatusException extends YdbExecutionException {

    private final StatusCode statusCode;

    public YdbExecutionStatusException(Object response, StatusCode statusCode) {
        super(String.valueOf(response), null, statusCode.getCode());
        this.statusCode = statusCode;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }
}
