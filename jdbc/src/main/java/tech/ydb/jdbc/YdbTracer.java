package tech.ydb.jdbc;


import tech.ydb.jdbc.impl.YdbTracerImpl;



/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbTracer {
    static void clear() {
        YdbTracerImpl.clear();
    }

    static YdbTracer current() {
        return YdbTracerImpl.current();
    }

    void setId(String id);

    void trace(String message);

    void query(String queryText);

    void markToPrint(String label);

    void close();

    default void markToPring() {
        markToPrint(null);
    }
}
