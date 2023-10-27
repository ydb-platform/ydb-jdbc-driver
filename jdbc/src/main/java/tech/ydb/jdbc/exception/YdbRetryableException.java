package tech.ydb.jdbc.exception;

import tech.ydb.core.Status;

public class YdbRetryableException extends YdbStatusException {
    private static final long serialVersionUID = 2082287790625648960L;

    YdbRetryableException(String message, String sqlState, Status status) {
        super(message, sqlState, status);
    }
}
