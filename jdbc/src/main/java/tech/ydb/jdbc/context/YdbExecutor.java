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
import tech.ydb.jdbc.exception.YdbExecutionException;
import tech.ydb.jdbc.exception.YdbStatusException;
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
        if (!status.isSuccess()) {
            throw YdbStatusException.newException(message, status);
        }
    }
}
