package tech.ydb.jdbc.exception;

import java.sql.SQLException;

public class YdbExecutionException extends SQLException {
    private static final long serialVersionUID = -9189855688894485591L;

    public YdbExecutionException(String reason) {
        super(reason);
    }

    public YdbExecutionException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public YdbExecutionException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }
}
