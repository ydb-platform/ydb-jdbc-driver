package tech.ydb.jdbc;

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

    void setId(String id);

    void trace(String message);

    void query(String queryText);

    void markToPrint(String label);

    void close();

    default void markToPrint() {
        markToPrint(null);
    }
}
