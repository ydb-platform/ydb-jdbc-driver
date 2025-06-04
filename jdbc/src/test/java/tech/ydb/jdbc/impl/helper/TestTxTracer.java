package tech.ydb.jdbc.impl.helper;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;

import tech.ydb.jdbc.YdbTracer;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TestTxTracer implements YdbTracer {
    private volatile String lastQuery;
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void setId(String id) {
    }

    @Override
    public void trace(String message) {
    }

    @Override
    public void query(String queryText) {
        counter.incrementAndGet();
        lastQuery = queryText;
    }

    @Override
    public void markToPrint(String label) {
    }

    @Override
    public void close() {
    }

    public String getQueryText() {
        return lastQuery;
    }

    public void assertQueriesCount(int count) {
        Assertions.assertEquals(count, counter.get(), "Incorrect count of queries");
        counter.set(0);
    }

    public void assertLastQueryContains(String query) {
        Assertions.assertNotNull(lastQuery, "Last query must be");
        Assertions.assertTrue(lastQuery.contains(query), "Query '" + lastQuery + "' must contain '" + query + "'");
    }
}
