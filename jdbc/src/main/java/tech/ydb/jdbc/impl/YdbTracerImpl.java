package tech.ydb.jdbc.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.jdbc.YdbTracer;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbTracerImpl implements YdbTracer {
    private static final Logger LOGGER = Logger.getLogger(YdbTracer.class.getName());
    private static final ThreadLocal<YdbTracer> LOCAL = new ThreadLocal<>();
    private static final AtomicLong ANONYMOUS_COUNTER = new AtomicLong(0);

    private class Record {
        private final long executedAt = System.currentTimeMillis();
        private final String message;
        private final boolean isRequest;

        Record(String message, boolean isRequest) {
            this.message = message;
            this.isRequest = isRequest;
        }
    }

    private class Tx {
        private final Date startDate = new Date();
        private final long startedAt = System.currentTimeMillis();
        private final List<Record> records = new ArrayList<>();

        private String id = null;
        private String label = null;
        private boolean isMarked = false;

        private void log(Level level) {
            if (!LOGGER.isLoggable(level) || records.isEmpty()) {
                return;
            }

            long finishedAt = System.currentTimeMillis();
            long requestsTime = 0;

            String idName = id != null ? id : "anonymous-" + ANONYMOUS_COUNTER.incrementAndGet();
            String traceID = label == null ? idName : label + "-" + idName;
            LOGGER.log(level, "Trace[{0}] started at {1}", new Object[] {traceID, startDate});
            long last = startedAt;
            long requestsCount = 0;
            boolean lastIsRequest = false;
            for (Record record: records) {
                if (record.isRequest) {
                    requestsCount++;
                    lastIsRequest = true;
                    if (record.message != null) {
                        String clean =  record.message.replaceAll("\\s", " ");
                        LOGGER.log(level, "Query[{0}] {1}", new Object[] {traceID, clean});
                    }
                } else {
                    long ms = record.executedAt - last;
                    if (lastIsRequest) {
                        requestsTime += ms;
                        lastIsRequest = false;
                    }
                    LOGGER.log(level, "Trace[{0}] {1} ms {2}", new Object[] {traceID, ms, record.message});
                    last = record.executedAt;
                }
            }
            LOGGER.log(level, "Trace[{0}] finished in {1} ms, {2} requests take {3} ms", new Object[] {
                traceID, finishedAt - startedAt, requestsCount, requestsTime
            });
        }
    }

    private Tx tx = null;

    public static <T extends YdbTracer> T use(T tracer) {
        LOCAL.set(tracer);
        return tracer;
    }

    public static YdbTracer get() {
        YdbTracer tracer = LOCAL.get();
        if (tracer == null) {
            tracer = new YdbTracerImpl();
            LOCAL.set(tracer);
        }

        return tracer;
    }

    public static void clear() {
        YdbTracer tracer = LOCAL.get();
        if (tracer != null) {
            tracer.close();
        }
        LOCAL.remove();
    }

    private Tx ensureOpen() {
        if (tx == null) {
            tx = new Tx();
        }
        return tx;
    }

    @Override
    public void trace(String message) {
        ensureOpen().records.add(new Record(message, false));
    }

    @Override
    public void query(String queryText) {
        ensureOpen().records.add(new Record(queryText, true));
    }

    @Override
    public void setId(String id) {
        Tx local = ensureOpen();
        if (!Objects.equals(id, local.id)) {
            local.id = id;
            trace("set-id " + id);
        }
    }

    @Override
    public void markToPrint(String label) {
        Tx local = ensureOpen();
        if (!local.isMarked || !Objects.equals(label, local.label)) {
            local.isMarked = true;
            local.label = label;
            trace("markToPrint " + label);
        }
    }

    @Override
    public void close() {
        if (tx != null) {
            tx.log(tx.isMarked ? Level.INFO : Level.FINE);
            tx = null;
        }
    }
}
