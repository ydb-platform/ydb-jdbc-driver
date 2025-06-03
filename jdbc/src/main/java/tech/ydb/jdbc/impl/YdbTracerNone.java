package tech.ydb.jdbc.impl;

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
    public void markToPrint() { }

    @Override
    public void setId(String id) { }
}
