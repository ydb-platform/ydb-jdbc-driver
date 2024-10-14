package tech.ydb.jdbc;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbTracer {
    private static final Logger LOGGER = Logger.getLogger(YdbTracer.class.getName());
    private static final ThreadLocal<YdbTracer> LOCAL = new ThreadLocal<>();

    private final Date startDate = new Date();
    private final long startedAt = System.currentTimeMillis();
    private final List<Record> records = new ArrayList<>();

    private String txID = null;
    private boolean isMarked = false;
    private boolean isClosed = false;

    private class Record {
        private final long executedAt = System.currentTimeMillis();
        private final String type;
        private final String comment;

        public Record(String type, String comment) {
            this.type = type;
            this.comment = comment;
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

    public void trace(String type, String comment) {
        records.add(new Record(type, comment));
    }

    public void setId(String id) {
        this.txID = id;
        trace("SET ID", id);
    }

    public void markToPrint() {
        this.isMarked = true;
        trace("MARK TO PRINT", "");
    }

    public void close() {
        isClosed = true;

        LOCAL.remove();

        final Level level = isMarked ? Level.INFO : Level.FINE;
        if (!LOGGER.isLoggable(level) || records.isEmpty()) {
            return;
        }

        long finishedAt = System.currentTimeMillis();

        final String id = txID != null ? txID : "UKNOWN-" + ThreadLocalRandom.current().nextLong();
        LOGGER.log(level, "Trace[{0}] started at {1}", new Object[] { id, startDate });
        long last = startedAt;
        for (Record record: records) {
            long ms = record.executedAt - last;
            LOGGER.log(level, "Trace[{0}] {1} ms {2}: {3}", new Object[] { id, ms, record.type, record.comment });
            last = record.executedAt;
        }
        LOGGER.log(level, "Trace[{0}] finished in {1} ms", new Object[] { id, finishedAt - startedAt });
    }
}
