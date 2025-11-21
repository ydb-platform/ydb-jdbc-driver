package tech.ydb.jdbc.impl;


import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.core.Issue;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcFlowControl;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.ColumnInfo;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.context.YdbValidator;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.query.result.QueryStats;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryResultReader extends YdbQueryResultBase implements GrpcFlowControl {
    private static final Logger LOGGER = Logger.getLogger(YdbQueryResultReader.class.getName());

    private final YdbTypes types;
    private final YdbStatement statement;
    private final int fetchSize;

    private final LazyRs[] rs;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition isReady = lock.newCondition();

    private int lastRsIndex = 0;
    private CallCtrl callFlow = null;
    private Runnable canceller = null;

    private volatile boolean isStreamCompleted = false;

    public YdbQueryResultReader(YdbTypes types, YdbStatement statement, YdbQuery query) {
        super(query, query.getStatements().size());
        this.types = types;
        this.statement = statement;
        this.fetchSize = statement.getFetchSize();
        this.rs = new LazyRs[query.getStatements().size()];
        for (int idx = 0; idx < rs.length; idx += 1) {
            rs[idx] = new LazyRs();
        }
    }

    private void waitForUpdates() throws SQLException {
        lock.lock();
        try {
            isReady.await(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            throw new SQLException(ex);
        } finally {
            lock.unlock();
        }
    }

    private void releaseWaiters() {
        lock.lock();
        try {
            isReady.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws SQLException {
        super.close();

        if (!isStreamCompleted) {
            waitForUpdates();
            if (!isStreamCompleted && canceller != null) {
                canceller.run();

                // wait of completing
                while (!isStreamCompleted) {
                    waitForUpdates();
                }
            }
        }
    }

    public boolean onRead(int index, ResultSetReader rsr) {
        int count = rsr.getRowCount();
        if (index < 0 || index >= rs.length || rs[index].isClosed) {
            LOGGER.log(Level.FINEST, "Skipped {0} rows", count);
            releaseWaiters();
            return fetchSize > 0;
        }

        for (int prev = lastRsIndex; prev < index; prev += 1) {
            rs[prev].isCompleted = true;
        }
        lastRsIndex = index;

        LOGGER.log(Level.FINEST, "Loaded {0} rows", count);
        callFlow.loadRows(count);
        rs[index].queue.offer(rsr);
        releaseWaiters();

        return fetchSize > 0 && callFlow.loaded.get() > fetchSize;
    }

    public void onClose(Status status, Throwable th) {
        isStreamCompleted = true;
        for (int idx = 0; idx < rs.length; idx += 1) {
            rs[idx].isCompleted = true;
        }
        releaseWaiters();
    }

    @Override
    public Call newCall(IntConsumer req) {
        callFlow = new CallCtrl(req);
        return callFlow;
    }

    @Override
    protected YdbResultSet getResultSet(int index) throws SQLException {
        if (index < 0 || index >= rs.length) {
            return null;
        }

        YdbResultSet ready = rs[index].getReady();

        while (ready == null && !isStreamCompleted) {
            ready = rs[index].getReady();
        }

        return ready;
    }

    @Override
    protected void closeResultSet(int index) throws SQLException {
        if (index < 0 || index >= rs.length) {
            return;
        }

        rs[index].close();
    }

    public CompletableFuture<Status> load(GrpcReadStream<ResultSetReader> stream) {
        CompletableFuture<Status> resultIsReady = new CompletableFuture<>();
        canceller = stream::cancel;

        stream.start(rsr -> {
            if (onRead(0, rsr)) {
                resultIsReady.complete(Status.SUCCESS);
            }
        }).whenComplete((status, th) -> {
            onClose(status, th);

            if (status != null) {
                resultIsReady.complete(status);
            } else {
                resultIsReady.complete(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th));
            }
        });

        return resultIsReady;
    }


    public CompletableFuture<Status> load(YdbValidator validator, QueryStream stream, Consumer<QueryStats> stats) {
        CompletableFuture<Status> resultIsReady = new CompletableFuture<>();
        canceller = stream::cancel;

        stream.execute(new QueryStream.PartsHandler() {
            @Override
            public void onIssues(Issue[] issues) {
                validator.addStatusIssues(Arrays.asList(issues));
            }

            @Override
            public void onNextPart(QueryResultPart part) {
                if (onRead((int) part.getResultSetIndex(), part.getResultSetReader())) {
                    resultIsReady.complete(Status.SUCCESS);
                }
            }
        }).whenComplete((result, th) -> {
            Status status = null;
            if (result != null) {
                status = result.getStatus();
                if (result.isSuccess() && result.getValue().hasStats()) {
                    stats.accept(result.getValue().getStats());
                }
            }
            onClose(status, th);

            if (status != null) {
                resultIsReady.complete(status);
            } else {
                resultIsReady.complete(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th));
            }
        });

        return resultIsReady;
    }

    private class CallCtrl implements GrpcFlowControl.Call {
        private final IntConsumer request;
        private final AtomicInteger loaded = new AtomicInteger(0);
        private final AtomicBoolean isPaused = new AtomicBoolean(false);

        CallCtrl(IntConsumer request) {
            this.request = request;
        }

        @Override
        public void onStart() {
            request.accept(1);
        }

        @Override
        public void onMessageRead() {
            if (fetchSize <= 0 || loaded.get() < fetchSize) {
                request.accept(1);
            } else {
                isPaused.set(true);
            }
        }

        public void loadRows(int rows) {
            loaded.addAndGet(rows);
        }

        public void processRows(int rows) {
            if (loaded.addAndGet(-rows) < fetchSize) {
                if (isPaused.compareAndSet(true, false)) {
                    request.accept(1);
                }
            }
        }
    }

    private class LazyRs {
        private final ConcurrentLinkedQueue<ResultSetReader> queue = new ConcurrentLinkedQueue<>();
        private YdbResultSet rs = null;
        private boolean isClosed = false;
        private boolean isCompleted = false;

        void close() throws SQLException {
            if (rs != null) {
                rs.close();
            }
            isClosed = true;
            isCompleted = true;
        }

        YdbResultSet getReady() throws SQLException {
            if (rs != null) {
                return rs;
            }

            while (!isCompleted && queue.isEmpty()) {
                waitForUpdates();
            }

            if (isCompleted && fetchSize <= 0) { // can use in memory result set
                rs = new YdbResultSetMemory(types, statement, queue.toArray(new ResultSetReader[0]));
                return rs;
            }

            if (queue.isEmpty()) {
                return null;
            }

            ResultSetReader first = queue.peek();
            if (first.getRowCount() == 0) {
                queue.remove();
            }

            ColumnInfo[] columns = ColumnInfo.fromResultSetReader(types, Objects.requireNonNull(first));
            rs = new YdbResultSetForwardOnly(statement, columns) {
                @Override
                protected boolean hasNext() throws SQLException {
                    while (!isCompleted && queue.isEmpty()) {
                        waitForUpdates();

                        ResultSetReader next = queue.peek();
                        if (next != null && next.getRowCount() == 0) {
                            queue.remove();
                        }
                    }

                    return !queue.isEmpty();
                }

                @Override
                protected ResultSetReader readNext() throws SQLException {
                    ResultSetReader next  = queue.poll();
                    LOGGER.log(Level.FINEST, "Processed {0} rows", next.getRowCount());
                    callFlow.processRows(next.getRowCount());
                    return next;
                }
            };
            return rs;
        }
    }
}
