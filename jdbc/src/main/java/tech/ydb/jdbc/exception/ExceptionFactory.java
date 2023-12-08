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

    public static SQLException createException(String message, UnexpectedResultException cause) {
        StatusCode code = cause.getStatus().getCode();
        String sqlState = getSQLState(code);
        int vendorCode = getVendorCode(code);

        // base retryable statuses are translated to SQLRecoverableException
        if (code.isRetryable(false)) {
            return new YdbRetryableException(message, sqlState, vendorCode, cause);
        }

        // transport problems are translated to SQLTransientConnectionException
        if (code == StatusCode.TRANSPORT_UNAVAILABLE || code == StatusCode.UNAVAILABLE) {
            return new YdbUnavailbaleException(message, sqlState, vendorCode, cause);
        }

        // timeouts are translated to SQLTimeoutException
        if (code == StatusCode.TIMEOUT ||
                code == StatusCode.CLIENT_DEADLINE_EXPIRED ||
                code == StatusCode.CLIENT_DEADLINE_EXCEEDED) {
            return new YdbTimeoutException(message, sqlState, vendorCode, cause);
        }

        // all others transient problems are translated to base SQLTransientException
        if (code.isRetryable(true)) {
            return new YdbConditionallyRetryableException(message, sqlState, vendorCode, cause);
        }

        return new YdbSQLException(message, sqlState, vendorCode, cause);
    }
}
