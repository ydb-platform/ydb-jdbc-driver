package tech.ydb.jdbc.exception;

import java.sql.SQLException;

import tech.ydb.core.Issue;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ExceptionFactory {
    private ExceptionFactory() { }

    /*
     SQLException
     |-> YdbSQLException
     |-> SQLRecoverableException
     |    |-> YdbRetryableException
     |-> SQLTransientException
     |    |-> YdbConditionallyRetryableException
     |    |-> SQLTransientConnectionException
     |    |    |-> YdbUnavailbaleException
     |    |-> SQLTransactionRollbackException
     |    |-> SQLTimeoutException
     |         |-> YdbTimeoutException
     |-> SQLNonTransientException
          |-> SQLDataException
          |-> SQLInvalidAuthorizationSpecException
          |-> SQLSyntaxErrorException
          |-> SQLIntegrityConstraintViolationException
          |-> SQLFeatureNotSupportedException
     */

    static String getSQLState(Status status) {
        if (status.getCode() == StatusCode.GENERIC_ERROR) {
            return "42000";  // General SQL syntax error
        }
        if (status.getCode() == StatusCode.PRECONDITION_FAILED) {
            if (hasIssue(status.getIssues(), "Conflict with existing key.")) {
                return "23505"; // dublicate keys SQL state value
            }
        }
        return null;
    }

    private static boolean hasIssue(Issue[] issues, String text) {
        if (issues == null || issues.length == 0) {
            return false;
        }
        for (Issue issue: issues) {
            if (text.equalsIgnoreCase(issue.getMessage()) || hasIssue(issue.getIssues(), text)) {
                return true;
            }
        }
        return false;
    }

    static int getVendorCode(StatusCode code) {
        return code.getCode();
    }

    public static SQLException createException(String message, UnexpectedResultException cause) {
        String sqlState = getSQLState(cause.getStatus());

        StatusCode code = cause.getStatus().getCode();
        int vendorCode = getVendorCode(code);

        // base retryable statuses are translated to SQLRecoverableException
        if (code.isRetryable(false)) {
            return new YdbRetryableException(message, sqlState, vendorCode, cause);
        }

        // transport problems are translated to SQLTransientConnectionException
        if (code == StatusCode.TRANSPORT_UNAVAILABLE) {
            return new YdbUnavailbaleException(message, sqlState, vendorCode, cause);
        }

        // timeouts are translated to SQLTimeoutException
        if (code == StatusCode.CLIENT_DEADLINE_EXPIRED || code == StatusCode.CLIENT_DEADLINE_EXCEEDED) {
            return new YdbTimeoutException(message, sqlState, vendorCode, cause);
        }

        // all others transient problems are translated to base SQLTransientException
        if (code.isRetryable(true) || code == StatusCode.TIMEOUT) {
            return new YdbConditionallyRetryableException(message, sqlState, vendorCode, cause);
        }

        return new YdbSQLException(message, sqlState, vendorCode, cause);
    }
}
