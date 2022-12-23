package tech.ydb.jdbc.exception;

import tech.ydb.core.StatusCode;

public class YdbExecutionStatusException extends YdbExecutionException {
    private static final long serialVersionUID = 6561276791540944088L;

    private final StatusCode statusCode;

    public YdbExecutionStatusException(Object response, StatusCode statusCode) {
        super(String.valueOf(response), null, statusCode.getCode());
        this.statusCode = statusCode;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }
}
