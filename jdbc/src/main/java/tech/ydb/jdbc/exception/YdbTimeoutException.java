package tech.ydb.jdbc.exception;

import java.sql.SQLTimeoutException;

import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbTimeoutException extends SQLTimeoutException {
    private static final long serialVersionUID = -6309565506198809222L;

    private final Status status;

    YdbTimeoutException(String message, String sqlState, int code, UnexpectedResultException cause) {
        super(message, sqlState, code, cause);
        this.status = cause.getStatus();
    }

    public Status getStatus() {
        return status;
    }
}
