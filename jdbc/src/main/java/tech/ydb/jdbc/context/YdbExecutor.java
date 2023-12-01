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
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.exception.ExceptionFactory;
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
            simpleExecute(msg, runnableSupplier);
            return;
        }

        logger.finest(msg);
        Stopwatch sw = Stopwatch.createStarted();

        try {
            simpleExecute(msg, runnableSupplier);
            logger.log(Level.FINEST, "[{0}] OK ", sw.stop());
        } catch (SQLException | RuntimeException ex) {
            logger.log(Level.FINE, "[{0}] {1} ", new Object[] { sw.stop(), ex.getMessage() });
            throw ex;
        }
    }

    public <T> T call(String msg, Supplier<CompletableFuture<Result<T>>> callSupplier) throws SQLException {
        if (!isDebug) {
            return simpleCall(msg, callSupplier);
        }

        logger.finest(msg);
        Stopwatch sw = Stopwatch.createStarted();

        try {
            T value = simpleCall(msg, callSupplier);
            logger.log(Level.FINEST, "[{0}] OK ", sw.stop());
            return value;
        } catch (SQLException | RuntimeException ex) {
            logger.log(Level.FINE, "[{0}] {1} ", new Object[] { sw.stop(), ex.getMessage() });
            throw ex;
        }
    }

    private <T> T simpleCall(String msg, Supplier<CompletableFuture<Result<T>>> supplier) throws SQLException {
        try {
            Result<T> result = supplier.get().join();
            issues.addAll(Arrays.asList(result.getStatus().getIssues()));
            return result.getValue();
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot call '" + msg + "' with " + ex.getStatus(), ex);
        }
    }

    private void simpleExecute(String msg, Supplier<CompletableFuture<Status>> supplier) throws SQLException {
        Status status = supplier.get().join();
        issues.addAll(Arrays.asList(status.getIssues()));
        if (!status.isSuccess()) {
            throw ExceptionFactory.createException("Cannot execute '" + msg + "' with " + status,
                    new UnexpectedResultException("Unexpected status", status));
        }
    }
}
