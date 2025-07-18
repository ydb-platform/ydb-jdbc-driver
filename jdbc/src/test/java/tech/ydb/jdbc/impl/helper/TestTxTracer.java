package tech.ydb.jdbc.impl.helper;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;

import tech.ydb.jdbc.impl.YdbTracerNone;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TestTxTracer extends YdbTracerNone {
    private volatile String lastQuery;
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void query(String queryText) {
        counter.incrementAndGet();
        lastQuery = queryText;
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
