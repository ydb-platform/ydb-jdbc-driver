package tech.ydb.jdbc.context;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.ColumnInfo;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.impl.YdbResultSetBase;
import tech.ydb.jdbc.query.QueryStatement;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class StreamQueryResult implements YdbQueryResult {
    private static final Logger LOGGER = Logger.getLogger(StreamQueryResult.class.getName());

    private static final int DDL_EXPRESSION = -1;
    private static final int UPDATE_EXPRESSION = -2;

    private final String msg;
    private final YdbTypes types;
    private final YdbStatement statement;
    private final Runnable streamStopper;

    private final CompletableFuture<Status> streamFuture = new CompletableFuture<>();
    private final AtomicBoolean streamCancelled = new AtomicBoolean(false);

    private final int[] resultIndexes;
    private final List<CompletableFuture<Result<LazyResultSet>>> resultFutures = new ArrayList<>();

    private int resultIndex = 0;
    private volatile boolean resultClosed = false;

    public StreamQueryResult(String msg, YdbTypes types, YdbStatement statement, YdbQuery query, Runnable stopper) {
        this.msg = msg;
        this.types = types;
        this.statement = statement;
        this.streamStopper = stopper;

        this.resultIndexes = new int[query.getStatements().size()];

        int idx = 0;
        for (QueryStatement exp : query.getStatements()) {
            if (exp.isDDL()) {
                resultIndexes[idx++] = DDL_EXPRESSION;
                continue;
            }
            if (exp.hasUpdateCount()) {
                resultIndexes[idx++] = UPDATE_EXPRESSION;
                continue;
            }

            if (exp.hasResults()) {
                resultIndexes[idx++] = resultFutures.size();
                resultFutures.add(new CompletableFuture<>());
            }
        }
    }

    public void onStreamResultSet(int index, ResultSetReader rsr) {
        CompletableFuture<Result<LazyResultSet>> future = resultFutures.get(index);

        if (!future.isDone()) {
            ColumnInfo[] columns = ColumnInfo.fromResultSetReader(types, rsr);
            LazyResultSet rs = new LazyResultSet(statement, columns);
            rs.addResultSet(rsr);
            if (future.complete(Result.success(rs))) {
                return;
            }
        }

        Result<LazyResultSet> res = future.join();
        if (res.isSuccess()) {
            res.getValue().addResultSet(rsr);
        }
    }

    public void onStreamFinished(Throwable th) {
        streamFuture.completeExceptionally(th);
        for (CompletableFuture<Result<LazyResultSet>> future: resultFutures) {
            future.completeExceptionally(th);
        }

        completeAllSets();
    }

    public void onStreamFinished(Status status) {
        for (CompletableFuture<Result<LazyResultSet>> future : resultFutures) {
            if (status.isSuccess()) {
                future.complete(Result.success(new LazyResultSet(statement, new ColumnInfo[0]), status));
            } else {
                future.complete(Result.fail(status));
            }
        }
        streamFuture.complete(status);

        completeAllSets();
    }

    private void completeAllSets() {
        for (CompletableFuture<Result<LazyResultSet>> future: resultFutures) {
            if (!future.isCompletedExceptionally()) {
                Result<LazyResultSet> rs = future.join();
                if (rs.isSuccess()) {
                    rs.getValue().complete();
                }
            }
        }
    }

    private void closeResultSet(int index) throws SQLException {
        try {
            CompletableFuture<Result<LazyResultSet>> future = resultFutures.get(index);
            if (future != null) {
                future.join().getValue().close();
            }
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot call '" + msg + "' with " + ex.getStatus(), ex);
        }
    }

    private void checkStream() {
        if (!resultClosed) {
            return;
        }

        if (!streamFuture.isDone() && streamCancelled.compareAndSet(false, true)) {
            LOGGER.log(Level.FINE, "Stream cancel");
            streamStopper.run();
        }
    }

    @Override
    public void close() throws SQLException {
        if (streamFuture.isDone() && resultClosed) {
            return;
        }

        LOGGER.log(Level.FINE, "Stream closing");

        resultClosed = true;
        Status status = streamFuture.join();

        if (streamCancelled.get()) {
            LOGGER.log(Level.FINE, "Stream canceled and finished with status {0}", status);
            return;
        }

        for (CompletableFuture<Result<LazyResultSet>> future: resultFutures) {
            if (future.isDone()) {
                Result<LazyResultSet> res = future.join();
                if (res.isSuccess()) {
                    res.getValue().close();
                }
            }
        }

        LOGGER.log(Level.FINE, "Stream closed with status {0}", status);
        if (!status.isSuccess()) {
            throw ExceptionFactory.createException("Cannot execute '" + msg + "' with " + status,
                    new UnexpectedResultException("Unexpected status", status)
            );
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (resultIndex >= resultIndexes.length) {
            return -1;
        }
        int index = resultIndexes[resultIndex];
        if (index == DDL_EXPRESSION) {
            return 0;
        }
        if (index == UPDATE_EXPRESSION) {
            return 1;
        }
        return -1;
    }

    @Override
    public YdbResultSet getCurrentResultSet() throws SQLException {
        if (resultIndex >= resultIndexes.length) {
            return null;
        }
        int index = resultIndexes[resultIndex];
        if (index < 0 || index >= resultFutures.size()) {
            return null;
        }

        try {
            return resultFutures.get(index).join().getValue();
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot call '" + msg + "' with " + ex.getStatus(), ex);
        }
    }

    @Override
    public YdbResultSet getGeneratedKeys() throws SQLException {
        // TODO: Implement
        return null;
    }

    @Override
    public boolean hasResultSets() throws SQLException {
        if (resultIndex >= resultIndexes.length) {
            return false;
        }
        return resultIndexes[resultIndex] >= 0;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (resultFutures == null || resultIndex >= resultFutures.size()) {
            return false;
        }

        switch (current) {
            case Statement.KEEP_CURRENT_RESULT:
                break;
            case Statement.CLOSE_CURRENT_RESULT:
                closeResultSet(resultIndex);
                break;

            case Statement.CLOSE_ALL_RESULTS:
                for (int idx = 0; idx <= resultIndex; idx += 1) {
                    closeResultSet(resultIndex);
                }
                break;
            default:
                throw new SQLException(YdbConst.RESULT_SET_MODE_UNSUPPORTED + current);
        }

        resultIndex += 1;
        return hasResultSets();

    }

    private class LazyResultSet extends YdbResultSetBase {
        private final BlockingQueue<ResultSetReader> readers = new ArrayBlockingQueue<>(5);
        private final AtomicLong rowsCount = new AtomicLong();
        private final CompletableFuture<Void> isCompleted = new CompletableFuture<>();
        private volatile boolean isClosed = false;

        private ResultSetReader current = null;
        private int rowIndex = 0;

        LazyResultSet(YdbStatement statement, ColumnInfo[] columns) {
            super(statement, columns);
        }

        public void cleanQueue() {
            boolean isEmpty = false;
            while (!isEmpty) {
                isEmpty = readers.poll() == null;
            }
        }

        public void addResultSet(ResultSetReader rsr) {
            try {
                do {
                    checkStream();
                } while (!readers.offer(rsr, 100, TimeUnit.MILLISECONDS));
            } catch (InterruptedException ex) {
                if (streamFuture.completeExceptionally(ex)) {
                    LOGGER.log(Level.WARNING, "LazyResultSet offer interrupted");
                    streamStopper.run();
                }
                return;
            }

            long total = rowsCount.addAndGet(rsr.getRowCount());
            LOGGER.log(Level.FINEST, "LazyResultSet got {0} rows", total);

            if (isClosed) {
                cleanQueue();
            }
        }

        @Override
        protected ValueReader getValue(int columnIndex) throws SQLException {
            if (current == null) {
                throw new SQLException(YdbConst.INVALID_ROW + rowIndex);
            }

            return current.getColumn(columnIndex);
        }

        @Override
        public boolean next() throws SQLException {
            while (true) {
                if (isClosed) {
                    return false;
                }

                if (current != null && current.next()) {
                    rowIndex++;
                    return true;
                }

                if (isCompleted.isDone() && readers.isEmpty()) {
                    current = null;
                    if (rowsCount.get() > 0) {
                        rowIndex = rowsCount.intValue() + 1;
                    }
                    return false;
                }

                try {
                    current = readers.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    throw new SQLException(ex);
                }
            }
        }

        public void complete() {
            isCompleted.complete(null);
        }

        @Override
        public void close() {
            isClosed = true;
            current = null;
            cleanQueue();
        }

        @Override
        public int getRow() throws SQLException {
            return rowIndex;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return isClosed;
        }

        @Override
        public boolean isBeforeFirst() throws SQLException {
            return rowsCount.get() > 0 && rowIndex < 1;
        }

        @Override
        public boolean isAfterLast() throws SQLException {
            isCompleted.join();
            return rowsCount.get() > 0 && rowIndex > rowsCount.intValue();
        }

        @Override
        public boolean isFirst() throws SQLException {
            return rowIndex == 1;
        }

        @Override
        public boolean isLast() throws SQLException {
            isCompleted.join();
            return rowsCount.get() > 0 && rowIndex == rowsCount.intValue();
        }

        @Override
        public void beforeFirst() throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }

        @Override
        public void afterLast() throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }

        @Override
        public boolean first() throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }

        @Override
        public boolean last() throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }

        @Override
        public boolean absolute(int row) throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }

        @Override
        public boolean relative(int rows) throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }

        @Override
        public boolean previous() throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }

        @Override
        public void setFetchDirection(int direction) throws SQLException {
            if (direction != ResultSet.FETCH_FORWARD) {
                throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
            }
        }

        @Override
        public int getFetchDirection() throws SQLException {
            return ResultSet.FETCH_FORWARD;
        }
    }
}
