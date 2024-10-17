package tech.ydb.jdbc;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbTracer {
    private static final Logger LOGGER = Logger.getLogger(YdbTracer.class.getName());
    private static final ThreadLocal<YdbTracer> LOCAL = new ThreadLocal<>();
    private static final AtomicLong ANONYMOUS_COUNTER = new AtomicLong(0);

    private final Date startDate = new Date();
    private final long startedAt = System.currentTimeMillis();
    private final List<Record> records = new ArrayList<>();

    private String txID = null;
    private String label = null;
    private boolean isMarked = false;
    private boolean isClosed = false;

    private class Record {
        private final long executedAt = System.currentTimeMillis();
        private final String message;
        private final boolean isRequest;

        Record(String message, boolean isRequest) {
            this.message = message;
            this.isRequest = isRequest;
        }
    }

    public static void clear() {
        LOCAL.remove();
    }

    public static YdbTracer current() {
        YdbTracer tracer = LOCAL.get();
        if (tracer == null || tracer.isClosed) {
            tracer = new YdbTracer();
            LOCAL.set(tracer);
        }

        return tracer;
    }

    public void trace(String message) {
        records.add(new Record(message, false));
    }

    public void traceRequest(String message) {
        records.add(new Record(message, true));
    }

    public void setId(String id) {
        if (!Objects.equals(id, txID)) {
            this.txID = id;
            trace("set-id " + id);
        }
    }

    public void markToPrint() {
        if (!isMarked) {
            this.isMarked = true;
            trace("markToPrint");
        }
    }

    public void markToPrint(String label) {
        if (!isMarked) {
            this.isMarked = true;
            this.label = label;
            trace("markToPrint " + label);
        }
    }

    public void close() {
        isClosed = true;

        LOCAL.remove();

        final Level level = isMarked ? Level.INFO : Level.FINE;
        if (!LOGGER.isLoggable(level) || records.isEmpty()) {
            return;
        }

        long finishedAt = System.currentTimeMillis();
        long requestsTime = 0;

        String id = txID != null ? txID : "anonymous-" + ANONYMOUS_COUNTER.incrementAndGet();
        String traceID = label == null ? id : label + "-" + id;
        LOGGER.log(level, "Trace[{0}] started at {1}", new Object[] {traceID, startDate});
        long last = startedAt;
        long requestsCount = 0;
        boolean lastIsRequest = false;
        for (Record record: records) {
            if (record.isRequest) {
                requestsCount++;
                lastIsRequest = true;
                if (record.message != null) {
                    LOGGER.log(level, "Query[{0}]: {1}", new Object[] {traceID, record.message.replaceAll("\\s", " ")});
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
