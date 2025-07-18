package tech.ydb.jdbc;

import java.time.Instant;
import java.util.List;

import tech.ydb.jdbc.impl.YdbTracerImpl;



/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbTracer {
    static YdbTracer current() {
        return YdbTracerImpl.get();
    }

    static void clear() {
        YdbTracerImpl.clear();
    }

    Instant getTxStartedAt();
    List<String> getTxRequests();

    void setId(String id);

    void trace(String message);

    void query(String queryText);

    void markToPrint(String label);

    void close();

    @Deprecated
    default void markToPrint() {
        markToPrint(null);
    }
}
