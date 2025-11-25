package tech.ydb.jdbc.impl.helper;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import tech.ydb.jdbc.exception.YdbSQLException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ExceptionAssert {
    private ExceptionAssert() { }

    public static void ydbException(String message, Executable exec) {
        YdbSQLException ex = Assertions.assertThrows(YdbSQLException.class, exec,
                "Invalid statement must throw YdbSQLException"
        );
        Assertions.assertTrue(ex.getMessage().contains(message),
                "YdbSQLException '" + ex.getMessage() + "' doesn't contain message '" + message + "'");
    }

    public static void sqlDataException(String message, Executable exec) {
        SQLDataException ex = Assertions.assertThrows(SQLDataException.class, exec,
                "Invalid statement must throw SQLDataException"
        );
        Assertions.assertEquals(message, ex.getMessage());
//        Assertions.assertTrue(ex.getMessage().contains(message),
//                "SQLDataException '" + ex.getMessage() + "' doesn't contain message '" + message + "'");
    }

    public static void sqlRecoverable(String message, Executable exec) {
        SQLRecoverableException ex = Assertions.assertThrows(SQLRecoverableException.class, exec,
                "Invalid statement must throw SQLRecoverableException"
        );
        Assertions.assertTrue(ex.getMessage().contains(message),
                "SQLRecoverableException '" + ex.getMessage() + "' doesn't contain message '" + message + "'");
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
