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
    private volatile boolean wasCommit = false;
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void query(String queryText) {
        if (queryText != null) {
            counter.incrementAndGet();
            lastQuery = queryText;
        } else {
            wasCommit = true;
        }
    }

    public String getQueryText() {
        return lastQuery;
    }

    public void assertQueriesCount(int count, boolean withCommit) {
        Assertions.assertEquals(count, counter.get(), "Incorrect count of queries");
        Assertions.assertEquals(withCommit, wasCommit, "Incorrect transaction status");
        counter.set(0);
        wasCommit = false;
    }

    public void assertLastQueryContains(String query) {
        Assertions.assertNotNull(lastQuery, "Last query must be");
        Assertions.assertTrue(lastQuery.contains(query), "Query '" + lastQuery + "' must contain '" + query + "'");
    }
}
