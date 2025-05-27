package tech.ydb.jdbc.query;

public enum QueryType {
    UNKNOWN,

    DECLARE,

    // DDL
    SCHEME_QUERY,

    // DML
    DATA_QUERY,
    SCAN_QUERY,

    // EXPLAIN
    EXPLAIN_QUERY,

    // BULK
    BULK_QUERY;
}
