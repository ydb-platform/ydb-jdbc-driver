package tech.ydb.jdbc;



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

    void setId(String id);

    void trace(String message);

    void query(String queryText);

    void markToPrint(String label);

    void close();

    default void markToPrint() {
        markToPrint(null);
    }
}
