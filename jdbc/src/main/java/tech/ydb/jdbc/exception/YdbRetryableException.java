package tech.ydb.jdbc.exception;

import java.sql.SQLRecoverableException;

import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;

public class YdbRetryableException extends SQLRecoverableException {
    private static final long serialVersionUID = -7171306648623023924L;
    private final Status status;

    YdbRetryableException(String message, String sqlState, int code, UnexpectedResultException cause) {
        super(message, sqlState, code, cause);
        this.status = cause.getStatus();
    }

    public Status getStatus() {
        return status;
    }
}
