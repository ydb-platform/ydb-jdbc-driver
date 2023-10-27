package tech.ydb.jdbc.exception;

import tech.ydb.core.Status;

public class YdbNonRetryableException extends YdbStatusException {
    private static final long serialVersionUID = 687247673341671225L;

    YdbNonRetryableException(String message, String sqlState, Status status) {
        super(message, sqlState, status);
    }
}
