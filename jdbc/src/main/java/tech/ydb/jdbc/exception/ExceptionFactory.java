package tech.ydb.jdbc.exception;

import java.sql.SQLException;

import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ExceptionFactory {
    static String getSQLState(StatusCode status) {
        // TODO: Add SQLSTATE message with order with https://en.wikipedia.org/wiki/SQLSTATE
        return null;
    }

    static int getVendorCode(StatusCode code) {
        return code.getCode();
    }

    public static SQLException createException(UnexpectedResultException cause) {
        StatusCode code = cause.getStatus().getCode();
        String sqlState = getSQLState(code);
        int vendorCode = getVendorCode(code);

        if (code.isRetryable(false)) {
            return new YdbRetryableException(cause.getMessage(), sqlState, vendorCode, cause);
        }
        if (code.isRetryable(true)) {
            return new YdbConditionallyRetryableException(cause.getMessage(), sqlState, vendorCode, cause);
        }

        return new YdbSQLException(cause.getMessage(), sqlState, vendorCode, cause);
    }
}
