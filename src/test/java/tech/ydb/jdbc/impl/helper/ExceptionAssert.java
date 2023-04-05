package tech.ydb.jdbc.impl.helper;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import tech.ydb.jdbc.exception.YdbConditionallyRetryableException;
import tech.ydb.jdbc.exception.YdbConfigurationException;
import tech.ydb.jdbc.exception.YdbExecutionException;
import tech.ydb.jdbc.exception.YdbNonRetryableException;
import tech.ydb.jdbc.exception.YdbResultTruncatedException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ExceptionAssert {
    private ExceptionAssert() { }

    public static void ydbConfiguration(String message, Executable exec) {
        YdbConfigurationException ex = Assertions.assertThrows(YdbConfigurationException.class, exec,
                "Invalid statement must throw YdbConfigurationException"
        );
        Assertions.assertEquals(message, ex.getMessage());
    }

    public static void ydbNonRetryable(String message, Executable exec) {
        YdbNonRetryableException ex = Assertions.assertThrows(YdbNonRetryableException.class, exec,
                "Invalid statement must throw YdbNonRetryableException"
        );
        Assertions.assertTrue(ex.getMessage().contains(message),
                "YdbNonRetryableException '" + ex.getMessage() + "' doesn't contain message '" + message + "'");
    }

    public static void ydbExecution(String message, Executable exec) {
        YdbExecutionException ex = Assertions.assertThrows(YdbExecutionException.class, exec,
                "Invalid statement must throw YdbExecutionException"
        );
        Assertions.assertEquals(message, ex.getMessage());
    }

    public static void ydbResultTruncated(String message, Executable exec) {
        YdbResultTruncatedException ex = Assertions.assertThrows(YdbResultTruncatedException.class, exec,
                "Invalid statement must throw YdbExecutionException"
        );
        Assertions.assertEquals(message, ex.getMessage());
    }

    public static void ydbConditionallyRetryable(String message, Executable exec) {
        YdbConditionallyRetryableException ex = Assertions.assertThrows(YdbConditionallyRetryableException.class, exec,
                "Invalid statement must throw YdbConditionallyRetryableException"
        );
        Assertions.assertTrue(ex.getMessage().contains(message),
                "YdbConditionallyRetryableException '" + ex.getMessage()
                        + "' doesn't contain message '" + message + "'");
    }

    public static void sqlFeatureNotSupported(String message, Executable exec) {
        SQLFeatureNotSupportedException ex = Assertions.assertThrows(SQLFeatureNotSupportedException.class, exec,
                "Invalid statement must throw SQLFeatureNotSupportedException"
        );
        Assertions.assertEquals(message, ex.getMessage());
    }

    public static void sqlException(String message, Executable exec) {
        SQLException ex = Assertions.assertThrows(SQLException.class, exec,
                "Invalid statement must throw SQLException"
        );
        Assertions.assertEquals(message, ex.getMessage());
    }
}
