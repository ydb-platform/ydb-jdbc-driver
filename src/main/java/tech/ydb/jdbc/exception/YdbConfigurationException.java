package tech.ydb.jdbc.exception;

import java.sql.SQLException;

public class YdbConfigurationException extends SQLException {
    private static final long serialVersionUID = -9023124863392765984L;

    public YdbConfigurationException(String message) {
        super(message);
    }

    public YdbConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
