package tech.ydb.jdbc.exception;

import java.sql.SQLTransientException;

import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;

public class YdbConditionallyRetryableException extends SQLTransientException {
    private static final long serialVersionUID = 2155728765762467203L;
    private final Status status;

    YdbConditionallyRetryableException(String message, String sqlState, int code, UnexpectedResultException cause) {
        super(message, sqlState, code, cause);
        this.status = cause.getStatus();
    }

    public Status getStatus() {
        return status;
    }
}
