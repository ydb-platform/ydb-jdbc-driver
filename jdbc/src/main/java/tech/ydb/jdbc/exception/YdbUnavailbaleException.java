package tech.ydb.jdbc.exception;

import java.sql.SQLTransientConnectionException;

import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbUnavailbaleException extends SQLTransientConnectionException {
    private static final long serialVersionUID = 7162301155514557562L;

    private final Status status;

    YdbUnavailbaleException(String message, String sqlState, int code, UnexpectedResultException cause) {
        super(message, sqlState, code, cause);
        this.status = cause.getStatus();
    }

    public Status getStatus() {
        return status;
    }

}
