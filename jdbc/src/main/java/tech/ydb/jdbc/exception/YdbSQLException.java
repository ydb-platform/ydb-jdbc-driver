package tech.ydb.jdbc.exception;

import java.sql.SQLException;

import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;

public class YdbSQLException extends SQLException {
    private static final long serialVersionUID = 6204553083196091739L;

    private final Status status;

    YdbSQLException(String message, String sqlState, int code, UnexpectedResultException cause) {
        super(message, sqlState, code, cause);
        this.status = cause.getStatus();
    }

    public Status getStatus() {
        return status;
    }
}
