package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Stopwatch;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.jdbc.exception.YdbConditionallyRetryableException;
import tech.ydb.jdbc.exception.YdbExecutionException;
import tech.ydb.jdbc.exception.YdbNonRetryableException;
import tech.ydb.jdbc.exception.YdbRetryableException;
import tech.ydb.table.Session;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbExecutor {
    @SuppressWarnings("NonConstantLogger")
    private final Logger logger;
    private final boolean isDebug;
    private final List<Issue> issues = new ArrayList<>();

    public YdbExecutor(Logger logger) {
        this.logger = logger;
        this.isDebug = logger.isLoggable(Level.FINE);
    }

    public void clearWarnings() {
        this.issues.clear();
    }

    public SQLWarning toSQLWarnings() {
        SQLWarning firstWarning = null;
        SQLWarning warning = null;
        for (Issue issue : issues) {
            SQLWarning nextWarning = new SQLWarning(issue.toString(), null, issue.getCode());
            if (firstWarning == null) {
                firstWarning = nextWarning;
            }
            if (warning != null) {
                warning.setNextWarning(nextWarning);
            }
            warning = nextWarning;
        }
        return firstWarning;
    }

    public Session createSession(YdbContext ctx) throws SQLException {
        Duration timeout = ctx.getOperationProperties().getSessionTimeout();
        return call("create session", () -> ctx.getTableClient().createSession(timeout));
    }

    public void execute(String msg, Supplier<CompletableFuture<Status>> runnableSupplier) throws SQLException {
        if (!isDebug) {
            simpleExecute(runnableSupplier);
            return;
        }

        logger.finest(msg);
        Stopwatch sw = Stopwatch.createStarted();

        try {
            simpleExecute(runnableSupplier);
            logger.log(Level.FINEST, "[{0}] OK ", sw.stop());
        } catch (SQLException | RuntimeException ex) {
            logger.log(Level.FINE, "[{0}] {1} ", new Object[] { sw.stop(), ex.getMessage() });
            throw ex;
        }
    }

    public <T> T call(String msg, Supplier<CompletableFuture<Result<T>>> callSupplier) throws SQLException {
        if (!isDebug) {
            return simpleCall(callSupplier);
        }

        logger.finest(msg);
        Stopwatch sw = Stopwatch.createStarted();

        try {
            T value = simpleCall(callSupplier);
            logger.log(Level.FINEST, "[{0}] OK ", sw.stop());
            return value;
        } catch (SQLException | RuntimeException ex) {
            logger.log(Level.FINE, "[{0}] {1} ", new Object[] { sw.stop(), ex.getMessage() });
            throw ex;
        }
    }

    private <T> T simpleCall(Supplier<CompletableFuture<Result<T>>> supplier) throws SQLException {
        try {
            Result<T> result = supplier.get().join();
            validate(result.getStatus().toString(), result.getStatus());
            return result.getValue();
        } catch (RuntimeException ex) {
            throw new YdbExecutionException(ex.getMessage(), ex);
        }
    }

    private void simpleExecute(Supplier<CompletableFuture<Status>> supplier) throws SQLException {
        try {
            Status status = supplier.get().join();
            validate(status.toString(), status);
        } catch (RuntimeException ex) {
            throw new YdbExecutionException(ex.getMessage(), ex);
        }
    }

    private void validate(String message, Status status) throws SQLException {
        issues.addAll(Arrays.asList(status.getIssues()));

        switch (status.getCode()) {
            case SUCCESS:
                return;

            case BAD_REQUEST:
            case INTERNAL_ERROR:
            case CLIENT_UNAUTHENTICATED:
            // gRPC reports, request is not authenticated
            // Maybe internal error, maybe some issue with token
            case UNAUTHORIZED:
            // Unauthorized by database
            case SCHEME_ERROR:
            case GENERIC_ERROR:
            case CLIENT_CALL_UNIMPLEMENTED:
            case UNSUPPORTED:
            case UNUSED_STATUS:
            case ALREADY_EXISTS:
                throw new YdbNonRetryableException(message, status.getCode());

            case ABORTED:
            case UNAVAILABLE:
            // Some of database parts are not available
            case OVERLOADED:
            // Database is overloaded, need to retry with exponential backoff
            case TRANSPORT_UNAVAILABLE:
            // Some issues with networking
            case CLIENT_RESOURCE_EXHAUSTED:
            // No resources to handle client request
            case NOT_FOUND:
            // Could be 'prepared query' issue, could be 'transaction not found'
            // Should be retries with new session
            case BAD_SESSION:
            // Retry with new session
            case SESSION_EXPIRED:
                // Retry with new session
                throw new YdbRetryableException(message, status.getCode());

            case CANCELLED:
            // Query was canceled due to query timeout (CancelAfter)
            // Query was definitely canceled by database
            case CLIENT_CANCELLED:
            case CLIENT_INTERNAL_ERROR:
                // Some unknown client side error, probably on transport layer
                throw new YdbConditionallyRetryableException(message, status.getCode());

            case UNDETERMINED:
            case TIMEOUT:
            // Database cannot respond in time, need to retry with exponential backoff
            case PRECONDITION_FAILED:
            case CLIENT_DEADLINE_EXCEEDED:
            // Query was canceled on transport layer
            case SESSION_BUSY:
            // Another query is executing already, retry with new session
            case CLIENT_DISCOVERY_FAILED:
            // Some issue with database endpoints discovery
            case CLIENT_LIMITS_REACHED:
                // Client side session limit was reached
                throw new YdbConditionallyRetryableException(message, status.getCode());
            default:
                throw new YdbNonRetryableException(message, status.getCode());
        }
    }
}
