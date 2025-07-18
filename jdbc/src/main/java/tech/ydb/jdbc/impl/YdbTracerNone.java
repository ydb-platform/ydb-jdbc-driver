package tech.ydb.jdbc.impl;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import tech.ydb.jdbc.YdbTracer;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbTracerNone implements YdbTracer {
    public static final YdbTracer DISABLED = new YdbTracerNone();

    @Override
    public void trace(String message) { }

    @Override
    public void query(String queryText) { }

    @Override
    public void close() { }

    @Override
    public void markToPrint(String label) { }

    @Override
    public void setId(String id) { }

    @Override
    public Instant getTxStartedAt() {
        return Instant.MIN;
    }

    @Override
    public List<String> getTxRequests() {
        return Collections.emptyList();
    }
}
