package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.sql.SQLWarning;
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

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbValidator {
    @SuppressWarnings("NonConstantLogger")
    private final Logger logger;
    private final boolean isDebug;
    private final List<Issue> issues = new ArrayList<>();

    public YdbValidator(Logger logger) {
        this.logger = logger;
        this.isDebug = logger.isLoggable(Level.FINE);
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

    public void addStatusIssues(Status status) {
        issues.addAll(Arrays.asList(status.getIssues()));
    }

    public void clearWarnings() {
        this.issues.clear();
    }

    public void execute(String msg, Supplier<CompletableFuture<Status>> fn) throws SQLException {
        if (!isDebug) {
            runImpl(msg, fn);
            return;
        }

        logger.finest(msg);
        Stopwatch sw = Stopwatch.createStarted();

        try {
            runImpl(msg, fn);
            logger.log(Level.FINEST, "[{0}] OK ", sw.stop());
        } catch (SQLException | RuntimeException ex) {
            logger.log(Level.FINE, "[{0}] {1} ", new Object[] {sw.stop(), ex.getMessage()});
            throw ex;
        }
    }

    public <R> R call(String msg, Supplier<CompletableFuture<Result<R>>> fn) throws SQLException {
        if (!isDebug) {
            return callImpl(msg, fn);
        }

        logger.finest(msg);
        Stopwatch sw = Stopwatch.createStarted();

        try {
            R value = callImpl(msg, fn);
            logger.log(Level.FINEST, "[{0}] OK ", sw.stop());
            return value;
        } catch (SQLException | RuntimeException ex) {
            logger.log(Level.FINE, "[{0}] {1} ", new Object[] {sw.stop(), ex.getMessage()});
            throw ex;
        }
    }

    private void runImpl(String msg, Supplier<CompletableFuture<Status>> fn) throws SQLException {
        Status status = fn.get().join();
        addStatusIssues(status);

        if (!status.isSuccess()) {
            throw ExceptionFactory.createException("Cannot execute '" + msg + "' with " + status,
                    new UnexpectedResultException("Unexpected status", status));
        }
    }

    private <R> R callImpl(String msg, Supplier<CompletableFuture<Result<R>>> fn) throws SQLException {
        try {
            Result<R> result = fn.get().join();
            addStatusIssues(result.getStatus());
            return result.getValue();
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot call '" + msg + "' with " + ex.getStatus(), ex);
        }
    }

}
