package tech.ydb.jdbc;

import tech.ydb.jdbc.impl.YdbTracerImpl;



/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbTracer {
    interface Storage {
        YdbTracer get();

        default void clear() {

        }
    }

    static YdbTracer current() {
        return YdbTracerImpl.ENABLED.get();
    }

    static void clear() {
        YdbTracerImpl.ENABLED.clear();
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
